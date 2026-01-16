import os
import sys
import json
import base64
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# =========================================
# 1. CONFIGURATION
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# GitHub Credentials
GITHUB_PAT = os.getenv("PAT") # Make sure this is in your .env or set manually
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"

# API Endpoints
GITHUB_CONTENTS_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"
GITHUB_BLOB_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/git/blobs/"
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# Backtest Settings
DAYS_TO_FETCH = 60

# =========================================
# 2. GITHUB UTILITIES (SCAN & DOWNLOAD)
# =========================================

def get_headers():
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_PAT:
        headers["Authorization"] = f"Bearer {GITHUB_PAT}"
    return headers

def scan_repository():
    """Lists all .json files in the repository."""
    print(f"Scanning repository {REPO_OWNER}/{REPO_NAME}...")
    try:
        r = requests.get(GITHUB_CONTENTS_URL, headers=get_headers())
        if r.status_code != 200:
            print(f"Failed to list repo contents: {r.status_code} {r.text}")
            return []
        
        files = r.json()
        model_files = [f for f in files if f['name'].endswith('.json')]
        print(f"Found {len(model_files)} model files.")
        return model_files
    except Exception as e:
        print(f"Scan error: {e}")
        return []

def download_blob(file_sha):
    """Downloads file content using the BLOB API (bypasses 1MB limit)."""
    url = f"{GITHUB_BLOB_URL}{file_sha}"
    try:
        r = requests.get(url, headers=get_headers())
        if r.status_code != 200:
            print(f"Blob download failed: {r.status_code}")
            return None
        
        data = r.json()
        content_b64 = data.get('content', '')
        if not content_b64:
            return None
            
        # Decode Base64
        json_str = base64.b64decode(content_b64).decode('utf-8')
        return json.loads(json_str)
    except Exception as e:
        print(f"Error downloading blob: {e}")
        return None

# =========================================
# 3. STRATEGY MODEL
# =========================================

class StrategyModel:
    def __init__(self, strategy_data):
        self.cfg = strategy_data['config']
        self.params = strategy_data['params']
        self.bucket_size = self.params['bucket_size']
        self.b_count = self.cfg['b_count']
        self.seq_len = self.cfg['s_len']
        self.model_type = self.cfg['model']
        self.abs_map = self._deserialize_map(self.params['abs_map'])
        self.der_map = self._deserialize_map(self.params['der_map'])

    def _deserialize_map(self, raw_map):
        clean_map = {}
        for k, v in raw_map.items():
            if k == "": tuple_key = ()
            else: tuple_key = tuple(map(int, k.split('|')))
            clean_map[tuple_key] = int(v)
        return clean_map

    def get_bucket(self, price):
        if self.bucket_size <= 0: return 0
        return int(price // self.bucket_size)

# =========================================
# 4. PREDICTION ENGINE
# =========================================

def get_single_prediction(mode, abs_map, der_map, a_seq, d_seq, last_val):
    if mode == "Absolute":
        if a_seq in abs_map: return abs_map[a_seq]
    elif mode == "Derivative":
        if d_seq in der_map: return last_val + der_map[d_seq]
    return None

def predict_strategy(strategy, price_sequence):
    # Discretize
    buckets = [strategy.get_bucket(p) for p in price_sequence]
    s_len = strategy.seq_len
    
    if len(buckets) < s_len: return None, 0
    
    # Context
    current_slice = buckets[-s_len:]
    a_seq = tuple(current_slice)
    last_val = a_seq[-1]
    
    d_seq = ()
    if s_len > 1:
        d_seq = tuple(a_seq[k] - a_seq[k-1] for k in range(1, len(a_seq)))

    # Lookup
    mode = strategy.model_type
    pred_val = None
    
    if mode == "Combined":
        pred_abs = get_single_prediction("Absolute", strategy.abs_map, strategy.der_map, a_seq, d_seq, last_val)
        pred_der = get_single_prediction("Derivative", strategy.abs_map, strategy.der_map, a_seq, d_seq, last_val)
        
        dir_abs = 1 if (pred_abs is not None and pred_abs > last_val) else (-1 if (pred_abs is not None and pred_abs < last_val) else 0)
        dir_der = 1 if (pred_der is not None and pred_der > last_val) else (-1 if (pred_der is not None and pred_der < last_val) else 0)
            
        if dir_abs != 0 and dir_der != 0 and dir_abs == dir_der: pred_val = pred_abs
        elif dir_abs != 0 and dir_der == 0: pred_val = pred_abs
        elif dir_der != 0 and dir_abs == 0: pred_val = pred_der
    else:
        pred_val = get_single_prediction(mode, strategy.abs_map, strategy.der_map, a_seq, d_seq, last_val)

    if pred_val is not None:
        if pred_val > last_val: return pred_val, 1
        elif pred_val < last_val: return pred_val, -1
            
    return None, 0

def get_ensemble_prediction(strategies, price_sequence):
    active_signals = []
    max_lookback = max(s.seq_len for s in strategies)
    if len(price_sequence) < max_lookback: return 0

    for strat in strategies:
        pred_val, direction = predict_strategy(strat, price_sequence)
        if direction != 0:
            active_signals.append({"dir": direction, "b_count": strat.b_count})
    
    if not active_signals: return 0

    directions = {x['dir'] for x in active_signals}
    if len(directions) > 1: return 0 # Conflict
        
    active_signals.sort(key=lambda x: x['b_count'])
    return active_signals[0]['dir']

# =========================================
# 5. MARKET DATA
# =========================================

def fetch_binance_data(symbol, interval="1m", days=60):
    print(f"   Fetching market data for {symbol}...")
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    all_klines = []
    
    while start_time < end_time:
        url = f"{BINANCE_API_URL}?symbol={symbol}&interval={interval}&startTime={start_time}&endTime={end_time}&limit=1000"
        try:
            r = requests.get(url)
            data = r.json()
            if not isinstance(data, list): break
            all_klines.extend([[x[0], float(x[4])] for x in data])
            start_time = data[-1][0] + 1
            time.sleep(0.05)
        except: break
    return all_klines

def resample_data(klines_1m, target_interval):
    if target_interval in ["1m", "1min"]: return [x[1] for x in klines_1m]
    df = pd.DataFrame(klines_1m, columns=['ts', 'price'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace=True)
    pandas_freq = target_interval.replace("m", "min").replace("h", "H")
    return df['price'].resample(pandas_freq).last().dropna().tolist()

# =========================================
# 6. MAIN LOOP
# =========================================

def main():
    if not GITHUB_PAT:
        print("WARNING: No PAT found in .env. Private repos will fail.")

    # 1. Scan Repo
    files = scan_repository()
    if not files:
        print("No strategy files found to process.")
        return

    print("\n" + "="*50)
    print(f"STARTING BATCH BACKTEST ({len(files)} files)")
    print("="*50 + "\n")

    results = []

    # 2. Iterate over every file found
    for f_meta in files:
        file_name = f_meta['name']
        file_sha = f_meta['sha']
        
        print(f"PROCESSING: {file_name}")
        
        # 3. Download via Blob API
        model_json = download_blob(file_sha)
        if not model_json:
            print("   -> Failed to download/parse.")
            continue
            
        asset = model_json.get('asset')
        interval = model_json.get('interval')
        
        if not asset or not interval:
            print("   -> Invalid JSON structure (missing asset/interval).")
            continue

        # 4. Init Model
        try:
            strategies = [StrategyModel(s) for s in model_json['strategies']]
        except Exception as e:
            print(f"   -> Error initializing strategies: {e}")
            continue
            
        # 5. Fetch Data & Backtest
        raw_data = fetch_binance_data(asset, "1m", DAYS_TO_FETCH)
        if not raw_data:
            print("   -> No market data found.")
            continue
            
        prices = resample_data(raw_data, interval)
        
        if len(prices) < 100:
            print(f"   -> Insufficient data after resampling ({len(prices)} candles).")
            continue

        # Simulation
        balance = 1000.0
        trades = 0
        wins = 0
        max_seq = max(s.seq_len for s in strategies)

        for i in range(max_seq, len(prices) - 1):
            history = prices[:i+1]
            signal = get_ensemble_prediction(strategies, history)
            
            if signal != 0:
                trades += 1
                trade_return = ((prices[i+1] - prices[i]) / prices[i]) * signal
                if trade_return > 0: wins += 1
                balance *= (1 + trade_return)
        
        acc = (wins / trades * 100) if trades > 0 else 0.0
        pnl = (balance - 1000.0) / 1000.0 * 100
        
        print(f"   -> RESULT: Acc: {acc:.2f}% | PnL: {pnl:.2f}% | Trades: {trades}")
        results.append({
            "File": file_name,
            "Accuracy": acc,
            "PnL": pnl,
            "Trades": trades
        })
        print("-" * 30)

    # Final Summary
    print("\n" + "="*50)
    print("FINAL SUMMARY")
    print("="*50)
    if results:
        df_res = pd.DataFrame(results)
        print(df_res.sort_values(by="PnL", ascending=False).to_string(index=False))
    else:
        print("No successful backtests.")

if __name__ == "__main__":
    main()
