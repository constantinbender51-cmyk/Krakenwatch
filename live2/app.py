import os
import json
import requests
import base64
import urllib.request
import time
from datetime import datetime

# =========================================
# 1. CONFIGURATION
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# =========================================
# 2. MODEL LOADER (Auto-Discovery)
# =========================================

class ModelLoader:
    def __init__(self, pat=None):
        self.headers = {
            "Authorization": f"Bearer {pat}", 
            "Accept": "application/vnd.github.v3+json"
        } if pat else {}

    def fetch_all_models(self):
        """Scans the repo and downloads ALL .json strategy files."""
        print(f"[*] Scanning repository: {REPO_OWNER}/{REPO_NAME}...")
        
        try:
            r = requests.get(GITHUB_API_URL, headers=self.headers)
            if r.status_code != 200:
                print(f"[!] Error listing files: Status {r.status_code}")
                return []
                
            files = r.json()
            model_files = [f for f in files if f['name'].endswith('.json')]
            
            if not model_files:
                print("[!] No JSON model files found in repository.")
                return []

            print(f"[*] Found {len(model_files)} model files. Downloading...")
            
            loaded_models = []
            for file_info in model_files:
                self._download_and_parse(file_info['url'], loaded_models)
                
            return loaded_models

        except Exception as e:
            print(f"[!] Critical Error during discovery: {e}")
            return []

    def _download_and_parse(self, url, container):
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                file_content = r.json()
                decoded = base64.b64decode(file_content['content']).decode('utf-8')
                data = json.loads(decoded)
                
                # Basic validation to ensure it's a valid strategy file
                if 'asset' in data and 'strategies' in data:
                    container.append(data)
                    print(f"    -> Loaded: {data['asset']} [{data['interval']}] (Acc: {data.get('holdout_stats', {}).get('accuracy', 0):.1f}%)")
                else:
                    print(f"    -> Skipped invalid file (missing keys)")
        except Exception as e:
            print(f"    -> Error downloading file: {e}")

# =========================================
# 3. LIVE DATA UTILS
# =========================================

def get_latest_prices(symbol, interval, limit=100):
    # Binance API requires converting "1m" -> "1m", "1h" -> "1h" (Standard)
    # If your model saves "1min", mapping might be needed. 
    # Based on training script, interval is saved as "1m", "5m", etc. which works directly.
    
    url = f"{BINANCE_API_URL}?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            return [float(x[4]) for x in data]
    except Exception as e:
        print(f"[!] Binance Error ({symbol}): {e}")
        return []

# =========================================
# 4. INFERENCE ENGINE
# =========================================

class InferenceEngine:
    def __init__(self, model_data):
        self.strategies = model_data['strategies']
        self.asset = model_data['asset']
        self.interval = model_data['interval']
        
    def _get_bucket(self, price, bucket_size):
        if bucket_size <= 0: return 0
        return int(price // bucket_size)

    def _lookup(self, map_data, key):
        return map_data.get(key)

    def predict(self, recent_prices):
        active_signals = []
        current_price = recent_prices[-1]

        for strat in self.strategies:
            cfg = strat['config']
            params = strat['params']
            
            s_len = cfg['s_len']
            b_count = cfg['b_count']
            model_type = cfg['model']
            b_size = params['bucket_size']
            
            # Need enough data
            if len(recent_prices) < s_len + 1: continue

            # Create Sequences
            window = recent_prices[-s_len:]
            buckets = [self._get_bucket(p, b_size) for p in window]
            
            a_seq_key = "|".join(map(str, buckets))
            
            d_seq_key = ""
            if s_len > 1:
                derivs = [buckets[k] - buckets[k-1] for k in range(1, len(buckets))]
                d_seq_key = "|".join(map(str, derivs))

            last_bucket = buckets[-1]
            
            # Prediction Lookup
            pred_val = None
            
            # Helper
            def get_val(mode):
                if mode == "Absolute":
                    return self._lookup(params['abs_map'], a_seq_key)
                elif mode == "Derivative":
                    change = self._lookup(params['der_map'], d_seq_key)
                    if change is not None: return last_bucket + change
                return None

            # Resolve Type
            if model_type == "Absolute":
                pred_val = get_val("Absolute")
            elif model_type == "Derivative":
                pred_val = get_val("Derivative")
            elif model_type == "Combined":
                p_abs = get_val("Absolute")
                p_der = get_val("Derivative")
                
                dir_abs = 0 if p_abs is None else (1 if p_abs > last_bucket else -1 if p_abs < last_bucket else 0)
                dir_der = 0 if p_der is None else (1 if p_der > last_bucket else -1 if p_der < last_bucket else 0)
                
                if dir_abs != 0 and dir_der != 0 and dir_abs == dir_der:
                    pred_val = p_abs 
                elif dir_abs != 0 and dir_der == 0:
                     pred_val = p_abs
                elif dir_der != 0 and dir_abs == 0:
                     pred_val = p_der

            # Generate Signal
            if pred_val is not None:
                diff = pred_val - last_bucket
                if diff != 0:
                    direction = 1 if diff > 0 else -1
                    est_price = pred_val * b_size
                    active_signals.append({
                        "dir": direction,
                        "b_count": b_count,
                        "est_price": est_price
                    })

        # --- ENSEMBLE LOGIC ---
        if not active_signals:
            return 0, current_price, "Neutral (No Signals)"

        # 1. Consensus Check (Conflict Resolution)
        directions = {x['dir'] for x in active_signals}
        if len(directions) > 1:
            return 0, current_price, f"Neutral (Conflict: {len(active_signals)} models)"
            
        # 2. Selection (Lowest Bucket Count wins)
        active_signals.sort(key=lambda x: x['b_count'])
        winner = active_signals[0]
        
        strength = len(active_signals)
        return winner['dir'], winner['est_price'], f"Signal Strength: {strength}/{len(self.strategies)}"

# =========================================
# 5. MAIN LOOP
# =========================================

def main():
    print("--- MULTI-ASSET MODEL INFERENCE ---")
    
    # 1. LOAD ALL MODELS
    loader = ModelLoader(pat=GITHUB_PAT)
    all_models_data = loader.fetch_all_models()
    
    if not all_models_data:
        print("No models loaded. Exiting.")
        return

    # Create engines for each loaded file
    engines = [InferenceEngine(m) for m in all_models_data]
    print(f"\n[*] Initialized {len(engines)} inference engines.\n")

    # 2. EXECUTE
    print(f"{'ASSET':<10} | {'TF':<5} | {'PRICE':<10} | {'ACTION':<10} | {'NOTE'}")
    print("-" * 65)

    for engine in engines:
        # Fetch live data for this specific asset/interval
        prices = get_latest_prices(engine.asset, engine.interval)
        
        if len(prices) < 50:
            print(f"{engine.asset:<10} | {engine.interval:<5} | {'N/A':<10} | ERROR      | Insufficient Data")
            continue
            
        direction, target_price, note = engine.predict(prices)
        current_price = prices[-1]
        
        # Formatting Output
        action = "HOLD"
        color = ""
        if direction == 1: 
            action = "BUY 🟢"
        elif direction == -1: 
            action = "SELL 🔴"
            
        print(f"{engine.asset:<10} | {engine.interval:<5} | {current_price:<10.4f} | {action:<10} | {note}")
        
        # Respect API Rate Limits slightly
        time.sleep(0.1)

if __name__ == "__main__":
    main()
