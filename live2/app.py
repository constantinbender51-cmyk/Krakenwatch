import os
import json
import base64
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# =========================================
# 1. CONFIGURATION
# =========================================

# GitHub Config (Matches your uploaded file)
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# Try to load PAT from environment (just like your app.py)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GITHUB_PAT = os.getenv("PAT") # Ensure your .env file is present or set this manually

# Binance Config
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
DAYS_TO_FETCH = 60

# =========================================
# 2. GITHUB DOWNLOADER
# =========================================

def download_model_from_github(asset, interval):
    """
    Connects to the repo, finds the file asset_interval.json, and returns the parsed JSON.
    """
    filename = f"{asset}_{interval}.json"
    url = GITHUB_API_URL + filename
    
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_PAT:
        headers["Authorization"] = f"Bearer {GITHUB_PAT}"
        print(f"Authenticated with PAT for {REPO_OWNER}/{REPO_NAME}")
    else:
        print("WARNING: No PAT found. Attempting public access...")

    print(f"Downloading {filename} from GitHub...")
    
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 404:
            print(f"Error: File '{filename}' not found in repo.")
            return None
        if r.status_code != 200:
            print(f"GitHub API Error: {r.status_code} - {r.text}")
            return None
            
        file_data = r.json()
        
        # GitHub API returns content in base64
        content_b64 = file_data.get("content", "")
        if not content_b64:
            print("Error: File content is empty.")
            return None
            
        json_str = base64.b64decode(content_b64).decode("utf-8")
        model_data = json.loads(json_str)
        
        print(f"Successfully downloaded model for {asset}!")
        return model_data
        
    except Exception as e:
        print(f"Download failed: {e}")
        return None

# =========================================
# 3. STRATEGY CLASSES
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
            if k == "":
                tuple_key = ()
            else:
                tuple_key = tuple(map(int, k.split('|')))
            clean_map[tuple_key] = int(v)
        return clean_map

    def get_bucket(self, price):
        if self.bucket_size <= 0: return 0
        return int(price // self.bucket_size)

# =========================================
# 4. PREDICTION LOGIC
# =========================================

def get_single_prediction(mode, abs_map, der_map, a_seq, d_seq, last_val):
    if mode == "Absolute":
        if a_seq in abs_map:
            return abs_map[a_seq]
    elif mode == "Derivative":
        if d_seq in der_map:
            pred_change = der_map[d_seq]
            return last_val + pred_change
    return None

def predict_strategy(strategy, price_sequence):
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
        
        dir_abs = 0
        if pred_abs is not None: dir_abs = 1 if pred_abs > last_val else -1 if pred_abs < last_val else 0
            
        dir_der = 0
        if pred_der is not None: dir_der = 1 if pred_der > last_val else -1 if pred_der < last_val else 0
            
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

    # Consensus Check
    directions = {x['dir'] for x in active_signals}
    if len(directions) > 1: return 0 
        
    # Tie-breaker (Lowest bucket count wins)
    active_signals.sort(key=lambda x: x['b_count'])
    return active_signals[0]['dir']

# =========================================
# 5. DATA FETCHING
# =========================================

def fetch_binance_data(symbol, interval="1m", days=60):
    print(f"Fetching last {days} days of {interval} data for {symbol}...")
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
        except Exception: break
    return all_klines

def resample_data(klines_1m, target_interval):
    if target_interval in ["1m", "1min"]: return [x[1] for x in klines_1m]
    df = pd.DataFrame(klines_1m, columns=['ts', 'price'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df.set_index('ts', inplace=True)
    pandas_freq = target_interval.replace("m", "min").replace("h", "H")
    return df['price'].resample(pandas_freq).last().dropna().tolist()

# =========================================
# 6. MAIN EXECUTION
# =========================================

def main():
    # 1. Select Asset
    asset = "BTCUSDT"   # <--- CHANGE THIS IF NEEDED
    interval = "15m"    # <--- CHANGE THIS IF NEEDED (must match a trained file in repo)

    print(f"--- INITIALIZING BACKTEST FOR {asset} [{interval}] ---")

    # 2. Download Model from GitHub
    model_json = download_model_from_github(asset, interval)
    
    if not model_json:
        print("Could not load model. Please check your Repo, Asset name, or PAT.")
        return

    # 3. Initialize Strategies
    strategies = [StrategyModel(s) for s in model_json['strategies']]
    
    # 4. Fetch Market Data
    raw_data = fetch_binance_data(asset, "1m", DAYS_TO_FETCH)
    prices = resample_data(raw_data, interval)
    
    if len(prices) < 100:
        print("Insufficient market data.")
        return

    # 5. Run Simulation
    print(f"\nSimulating trades on {len(prices)} candles...")
    balance = 1000.0
    trades = 0
    wins = 0
    max_seq = max(s.seq_len for s in strategies)

    for i in range(max_seq, len(prices) - 1):
        history = prices[:i+1]
        next_price = prices[i+1]
        
        signal = get_ensemble_prediction(strategies, history)
        
        if signal != 0:
            trades += 1
            pct_change = (next_price - history[-1]) / history[-1]
            trade_return = pct_change * signal
            
            if trade_return > 0: wins += 1
            balance *= (1 + trade_return)

    # 6. Results
    print(f"\n=== RESULTS: {asset} ===")
    print(f"Trades: {trades}")
    print(f"Accuracy: {(wins/trades*100) if trades else 0:.2f}%")
    print(f"Final Balance: ${balance:.2f}")

if __name__ == "__main__":
    main()
