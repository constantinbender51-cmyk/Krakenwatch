import os
import json
import asyncio
import aiohttp
import requests
import time
from datetime import datetime, timedelta

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

# Binance Public API (No auth needed for market data)
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# =========================================
# 2. MODEL LOADER (Run Once)
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
                print("[!] No JSON model files found.")
                return []

            print(f"[*] Found {len(model_files)} files. Downloading into RAM...")
            
            loaded_models = []
            for file_info in model_files:
                raw_url = file_info.get('download_url')
                if raw_url:
                    self._download_raw(raw_url, file_info['name'], loaded_models)
            
            return loaded_models

        except Exception as e:
            print(f"[!] Critical Error during discovery: {e}")
            return []

    def _download_raw(self, url, filename, container):
        try:
            r = requests.get(url, headers=self.headers, stream=True)
            if r.status_code == 200:
                data = r.json()
                if 'asset' in data:
                    size_mb = len(r.content) / (1024 * 1024)
                    container.append(data)
                    print(f"    -> Loaded: {data['asset']:<10} | {size_mb:.2f} MB")
        except Exception as e:
            print(f"    -> Error downloading {filename}: {e}")

# =========================================
# 3. INFERENCE LOGIC (CPU Bound)
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

        # Ensure we have enough data for the longest strategy
        # (Assuming max sequence length is reasonable, e.g. < 20)
        
        for strat in self.strategies:
            cfg = strat['config']
            params = strat['params']
            
            s_len = cfg['s_len']
            b_count = cfg['b_count']
            model_type = cfg['model']
            b_size = params['bucket_size']
            
            if len(recent_prices) < s_len + 1: continue

            window = recent_prices[-s_len:]
            buckets = [self._get_bucket(p, b_size) for p in window]
            
            # Key Generation
            a_seq_key = "|".join(map(str, buckets))
            d_seq_key = ""
            if s_len > 1:
                derivs = [buckets[k] - buckets[k-1] for k in range(1, len(buckets))]
                d_seq_key = "|".join(map(str, derivs))
            
            last_bucket = buckets[-1]
            
            # Lookup Logic
            pred_val = None
            def get_val(mode):
                if mode == "Absolute": return self._lookup(params['abs_map'], a_seq_key)
                elif mode == "Derivative":
                    chg = self._lookup(params['der_map'], d_seq_key)
                    return last_bucket + chg if chg is not None else None
                return None

            if model_type == "Absolute": pred_val = get_val("Absolute")
            elif model_type == "Derivative": pred_val = get_val("Derivative")
            elif model_type == "Combined":
                p_abs, p_der = get_val("Absolute"), get_val("Derivative")
                dir_abs = 0 if p_abs is None else (1 if p_abs > last_bucket else -1 if p_abs < last_bucket else 0)
                dir_der = 0 if p_der is None else (1 if p_der > last_bucket else -1 if p_der < last_bucket else 0)
                
                if dir_abs != 0 and dir_der != 0 and dir_abs == dir_der: pred_val = p_abs 
                elif dir_abs != 0 and dir_der == 0: pred_val = p_abs
                elif dir_der != 0 and dir_abs == 0: pred_val = p_der

            if pred_val is not None:
                diff = pred_val - last_bucket
                if diff != 0:
                    active_signals.append({
                        "dir": 1 if diff > 0 else -1,
                        "b_count": b_count,
                        "est_price": pred_val * b_size
                    })

        # Aggregation
        if not active_signals: return 0, current_price, "Neutral"
        
        directions = {x['dir'] for x in active_signals}
        if len(directions) > 1: return 0, current_price, f"Conflict ({len(active_signals)})"
            
        active_signals.sort(key=lambda x: x['b_count'])
        return active_signals[0]['dir'], active_signals[0]['est_price'], f"Signal ({len(active_signals)})"

# =========================================
# 4. ASYNC ORCHESTRATOR
# =========================================

async def fetch_price(session, engine):
    """Async fetch for a single asset."""
    url = f"{BINANCE_API_URL}?symbol={engine.asset}&interval={engine.interval}&limit=50"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                # Parse close prices: [timestamp, open, high, low, close...]
                prices = [float(x[4]) for x in data]
                return engine, prices
    except Exception as e:
        pass
    return engine, []

async def run_bot_loop(engines):
    print("\n" + "="*60)
    print(f"🚀 BOT STARTED: {len(engines)} Assets | High-Frequency Mode")
    print("="*60 + "\n")

    async with aiohttp.ClientSession() as session:
        while True:
            # 1. CLOCK SYNC (Wait for next minute :00)
            now = datetime.now()
            # Calculate seconds to sleep to hit XX:XX:01 (1s buffer for candle close)
            # Or XX:XX:00.1 if you are very aggressive.
            sleep_seconds = 60 - now.second - (now.microsecond / 1_000_000.0) + 0.1
            if sleep_seconds < 0: sleep_seconds += 60
            
            print(f"⏳ Waiting {sleep_seconds:.2f}s for candle close...")
            await asyncio.sleep(sleep_seconds)

            # 2. START OF MINUTE
            start_ts = datetime.now()
            print(f"\n--- ⚡ SCAN: {start_ts.strftime('%H:%M:%S.%f')[:-3]} ---")

            # 3. ASYNC FETCH (All at once)
            tasks = [fetch_price(session, eng) for eng in engines]
            results = await asyncio.gather(*tasks)
            
            fetch_ts = datetime.now()
            latency_ms = (fetch_ts - start_ts).total_seconds() * 1000
            print(f"    Data Recv: {latency_ms:.0f}ms | Processing...")

            # 4. PROCESS SIGNALS
            signals = []
            for engine, prices in results:
                if not prices: continue
                
                direction, target, note = engine.predict(prices)
                
                if direction != 0:
                    signals.append((engine.asset, direction, prices[-1], note))

            # 5. OUTPUT
            if signals:
                print(f"{'ASSET':<10} {'ACTION':<8} {'PRICE':<10} {'NOTE'}")
                print("-" * 45)
                for s in signals:
                    action = "BUY 🟢" if s[1] == 1 else "SELL 🔴"
                    print(f"{s[0]:<10} {action:<8} {s[2]:<10.4f} {s[3]}")
            else:
                print("    No trade signals this interval.")
            
            end_ts = datetime.now()
            total_ms = (end_ts - start_ts).total_seconds() * 1000
            print(f"    Done. Total Latency: {total_ms:.0f}ms")

# =========================================
# 5. MAIN ENTRY POINT
# =========================================

def main():
    # 1. Sync Load (Heavy lifting)
    loader = ModelLoader(pat=GITHUB_PAT)
    raw_models = loader.fetch_all_models()
    
    if not raw_models:
        print("Exiting: No models found.")
        return

    # 2. Prepare Engines
    engines = [InferenceEngine(m) for m in raw_models]

    # 3. Enter Async Loop
    try:
        asyncio.run(run_bot_loop(engines))
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user.")

if __name__ == "__main__":
    main()
