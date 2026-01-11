import os
import json
import time
import requests
import threading
import schedule
import pandas as pd
import urllib.request
from datetime import datetime, timedelta
from collections import Counter
from flask import Flask, jsonify, render_template_string

# --- CONFIGURATION ---
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# GitHub Config
GITHUB_PAT = os.getenv("PAT") # Ensure this is in your .env file
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "Models"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# Binance Config
ASSETS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", 
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT"
]
BASE_INTERVAL = "15m"
TIMEFRAMES = {
    "15m": None,
    "30m": "30min",
    "60m": "1h",
    "240m": "4h",
    "1d": "1D"
}

# Global State for Server
SIGNAL_MATRIX = {} # { "BTCUSDT": { "15m": 1, "60m": 0, ... }, ... }
LAST_UPDATE = "Never"

# --- 1. UTILITIES ---

def get_bucket(price, bucket_size):
    if bucket_size <= 0: bucket_size = 1e-9
    return int(price // bucket_size)

def parse_map_key(key_str):
    """Converts JSON string key '1|2|3' back to tuple (1, 2, 3)"""
    try:
        return tuple(map(int, key_str.split('|')))
    except:
        return ()

def deserialize_map(json_map):
    """Converts the JSON-loaded map back to {tuple: Counter}"""
    clean_map = {}
    for k, v in json_map.items():
        clean_map[parse_map_key(k)] = Counter(v)
    return clean_map

def resample_prices(raw_data, target_freq):
    """Resamples (Close_Time, Price) tuples to target frequency."""
    if not raw_data: return []
    if target_freq is None:
        return [x[1] for x in raw_data]
    
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Resample and forward fill or drop na
    resampled = df['price'].resample(target_freq).last().dropna()
    return resampled.tolist()

# --- 2. DATA DOWNLOADERS ---

def fetch_models_from_github():
    """Downloads all strategy JSON files from the repo."""
    print("--- Downloading Strategies from GitHub ---")
    headers = {"Authorization": f"Bearer {GITHUB_PAT}"} if GITHUB_PAT else {}
    
    models_cache = {}
    
    for asset in ASSETS:
        models_cache[asset] = {}
        for tf in TIMEFRAMES.keys():
            filename = f"{asset}_{tf}.json"
            url = GITHUB_API_URL + filename
            
            try:
                # Get file metadata to find download URL
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    download_url = resp.json().get("download_url")
                    file_content = requests.get(download_url).json()
                    
                    # Deserialize immediately
                    strategies = []
                    for s in file_content.get("strategy_union", []):
                        params = s["trained_parameters"]
                        strategies.append({
                            "config": s["config"],
                            "bucket_size": params["bucket_size"],
                            "seq_len": params["seq_len"],
                            "abs_map": deserialize_map(params["abs_map"]),
                            "der_map": deserialize_map(params["der_map"]),
                            "all_vals": params["all_vals"],
                            "all_changes": params["all_changes"]
                        })
                    
                    models_cache[asset][tf] = strategies
                    print(f"[{asset}] Loaded {tf} models.")
                else:
                    print(f"[{asset}] No model found for {tf} (Status {resp.status_code})")
            except Exception as e:
                print(f"Error downloading {filename}: {e}")
                
    return models_cache

def get_binance_recent_data(symbol, days=60):
    """Fetches last N days of 15m data."""
    end_ts = int(time.time() * 1000)
    start_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={BASE_INTERVAL}&startTime={start_ts}&endTime={end_ts}&limit=1000"
    
    all_candles = []
    
    # Simple pagination for 2 months
    current_start = start_ts
    while current_start < end_ts:
        batch_url = f"{url}&startTime={current_start}"
        try:
            with urllib.request.urlopen(batch_url) as response:
                data = json.loads(response.read().decode())
                if not data: break
                
                batch = [(int(c[6]), float(c[4])) for c in data]
                all_candles.extend(batch)
                
                last_time = data[-1][6]
                if last_time >= end_ts - 1000: break
                current_start = last_time + 1
        except Exception as e:
            print(f"Binance API Error: {e}")
            break
            
    return all_candles

# --- 3. INFERENCE ENGINE ---

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val, all_vals, all_changes):
    """Replicates the prediction logic from the training script."""
    import random
    
    if model_type == "Absolute":
        if a_seq in abs_map: return abs_map[a_seq].most_common(1)[0][0]
        return random.choice(all_vals)
    elif model_type == "Derivative":
        if d_seq in der_map: pred_change = der_map[d_seq].most_common(1)[0][0]
        else: pred_change = random.choice(all_changes)
        return last_val + pred_change
    elif model_type == "Combined":
        abs_cand = abs_map.get(a_seq, Counter())
        der_cand = der_map.get(d_seq, Counter())
        poss = set(abs_cand.keys())
        for c in der_cand.keys(): poss.add(last_val + c)
        
        if not poss: return random.choice(all_vals)
        
        best, max_s = None, -1
        for v in poss:
            s = abs_cand[v] + der_cand[v - last_val]
            if s > max_s: max_s, best = s, v
        return best
    return last_val

def generate_signal(prices, strategies):
    """
    Takes a price list and a list of strategies (committee).
    Returns 1 (Buy), -1 (Sell), 0 (Neutral).
    """
    if not strategies or len(prices) < 50: return 0

    active_directions = []
    
    # We only predict the NEXT move based on the LATEST data
    # We need the max seq_len to ensure we have enough data
    max_seq_len = max(s['seq_len'] for s in strategies)
    if len(prices) < max_seq_len + 1: return 0

    for model in strategies:
        b_size = model['bucket_size']
        seq_len = model['seq_len']
        
        # Bucketize recent history only
        relevant_prices = prices[-(seq_len+1):]
        buckets = [get_bucket(p, b_size) for p in relevant_prices]
        
        # Current state sequence
        a_seq = tuple(buckets[:-1]) # Input sequence (excluding current "bucket" if we were training, but for inference we use up to NOW)
        # Actually, to predict TOMORROW, we use data up to TODAY.
        # So input sequence is buckets[-(seq_len):]
        a_seq = tuple(buckets[-seq_len:])
        
        if seq_len > 1:
            d_seq = tuple(a_seq[k] - a_seq[k-1] for k in range(1, len(a_seq)))
        else:
            d_seq = ()
            
        last_val = a_seq[-1]
        
        pred_val = get_prediction(
            model['config']['model_type'], 
            model['abs_map'], model['der_map'], 
            a_seq, d_seq, last_val, 
            model['all_vals'], model['all_changes']
        )
        
        pred_diff = pred_val - last_val
        
        if pred_diff != 0:
            direction = 1 if pred_diff > 0 else -1
            active_directions.append(direction)
            
    if not active_directions: return 0
    
    up_votes = active_directions.count(1)
    down_votes = active_directions.count(-1)
    
    if up_votes > down_votes: return 1
    elif down_votes > up_votes: return -1
    return 0

# --- 4. BACKTESTING & LIVE LOOP ---

def run_backtest(models_cache):
    """Runs a quick backtest on the downloaded data."""
    print("\n--- Starting Backtest (Last 60 Days) ---")
    results = {}
    
    for asset in ASSETS:
        raw_data = get_binance_recent_data(asset, days=60)
        results[asset] = {}
        
        for tf_name, tf_pandas in TIMEFRAMES.items():
            if tf_name not in models_cache[asset]: continue
            
            strategies = models_cache[asset][tf_name]
            prices = resample_prices(raw_data, tf_pandas)
            
            # Simulate walking forward
            balance = 0
            trades = 0
            wins = 0
            
            # Start half-way through data to save time
            start_idx = len(prices) // 2 
            
            for i in range(start_idx, len(prices)-1):
                # Historical context up to i
                hist_prices = prices[:i+1]
                
                sig = generate_signal(hist_prices, strategies)
                
                if sig != 0:
                    trades += 1
                    # Did price move in that direction?
                    actual_move = prices[i+1] - prices[i]
                    if (sig == 1 and actual_move > 0) or (sig == -1 and actual_move < 0):
                        wins += 1
                        
            win_rate = (wins/trades*100) if trades > 0 else 0
            results[asset][tf_name] = f"{win_rate:.1f}% ({trades} trds)"
            print(f"Backtest {asset} {tf_name}: {win_rate:.1f}% Win Rate over {trades} trades")
            
    return results

def update_live_signals(models_cache):
    global SIGNAL_MATRIX, LAST_UPDATE
    print(f"\n[Scheduler] Updating signals at {datetime.now().strftime('%H:%M')}...")
    
    temp_matrix = {}
    
    for asset in ASSETS:
        temp_matrix[asset] = {}
        # Fetch fresh data (just last 2 days is enough for inference)
        raw_data = get_binance_recent_data(asset, days=5) 
        
        for tf_name, tf_pandas in TIMEFRAMES.items():
            if tf_name not in models_cache.get(asset, {}): 
                temp_matrix[asset][tf_name] = "N/A"
                continue
                
            prices = resample_prices(raw_data, tf_pandas)
            strategies = models_cache[asset][tf_name]
            
            # Generate Signal on LATEST data
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[Scheduler] Signals Updated.")

# --- 5. FLASK SERVER ---

app = Flask(__name__)

@app.route('/')
def home():
    html = f"""
    <html>
    <head>
        <title>Strategy Matrix</title>
        <meta http-equiv="refresh" content="60">
        <style>
            body {{ font-family: monospace; background: #111; color: #eee; padding: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #333; padding: 10px; text-align: center; }}
            th {{ background: #222; }}
            .buy {{ background: #004400; color: #0f0; }}
            .sell {{ background: #440000; color: #f00; }}
            .neutral {{ color: #555; }}
        </style>
    </head>
    <body>
        <h1>Live Signal Matrix</h1>
        <p>Last Update: {LAST_UPDATE}</p>
        <table>
            <thead>
                <tr>
                    <th>Asset</th>
                    {''.join(f'<th>{tf}</th>' for tf in TIMEFRAMES)}
                </tr>
            </thead>
            <tbody>
    """
    
    for asset in ASSETS:
        row = f"<tr><td>{asset}</td>"
        signals = SIGNAL_MATRIX.get(asset, {})
        for tf in TIMEFRAMES:
            val = signals.get(tf, 0)
            cls = "neutral"
            txt = "WAIT"
            if val == 1: 
                cls = "buy"
                txt = "BUY"
            elif val == -1: 
                cls = "sell"
                txt = "SELL"
            elif val == "N/A":
                txt = "-"
                
            row += f"<td class='{cls}'>{txt}</td>"
        row += "</tr>"
        html += row
        
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    return render_template_string(html)

@app.route('/json')
def json_api():
    return jsonify({"timestamp": LAST_UPDATE, "signals": SIGNAL_MATRIX})

# --- MAIN ENTRY ---

def run_scheduler(models_cache):
    # Initial run
    update_live_signals(models_cache)
    # Schedule every 15 mins
    schedule.every(15).minutes.do(update_live_signals, models_cache)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # 1. Download Models
    models = fetch_models_from_github()
    
    # 2. Run Backtest (Optional, runs once on startup)
    run_backtest(models)
    
    # 3. Start Scheduler in Background Thread
    t = threading.Thread(target=run_scheduler, args=(models,))
    t.daemon = True
    t.start()
    
    # 4. Start Web Server
    print("Starting Web Server on port 8080...")
    app.run(host='0.0.0.0', port=8080)
              
