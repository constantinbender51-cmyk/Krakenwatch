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

GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "Models"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

ASSETS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", 
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT"
]

# Validation Dates (Must match Training Script)
START_DATE = "2020-01-01"
END_DATE = "2026-01-01"

BASE_INTERVAL = "15m"
TIMEFRAMES = {
    "15m": None,
    "30m": "30min",
    "60m": "1h",
    "240m": "4h",
    "1d": "1D"
}

# Global State
SIGNAL_MATRIX = {}
HISTORY_LOG = []  # Stores last 7 days of signals
LAST_UPDATE = "Never"

# --- 1. UTILITIES ---

def get_bucket(price, bucket_size):
    if bucket_size <= 0: bucket_size = 1e-9
    return int(price // bucket_size)

def parse_map_key(key_str):
    try:
        return tuple(map(int, key_str.split('|')))
    except:
        return ()

def deserialize_map(json_map):
    """Converts the JSON-loaded map back to {tuple: Counter(int: int)}"""
    clean_map = {}
    for k, v in json_map.items():
        int_counter = Counter({int(ik): iv for ik, iv in v.items()})
        clean_map[parse_map_key(k)] = int_counter
    return clean_map

def resample_prices(raw_data, target_freq):
    if not raw_data: return []
    if target_freq is None:
        return [x[1] for x in raw_data]
    
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    resampled = df['price'].resample(target_freq).last().dropna()
    return resampled.tolist()

def get_resampled_df(raw_data, target_freq):
    """Returns DataFrame with timestamps for historical mapping"""
    if not raw_data: return pd.DataFrame()
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    if target_freq is None: return df
    return df['price'].resample(target_freq).last().dropna().to_frame()

# --- 2. DATA DOWNLOADERS (FULL HISTORY) ---

def fetch_models_from_github():
    print("--- Downloading Strategies from GitHub ---")
    headers = {"Authorization": f"Bearer {GITHUB_PAT}"} if GITHUB_PAT else {}
    models_cache = {}
    
    for asset in ASSETS:
        models_cache[asset] = {}
        for tf in TIMEFRAMES.keys():
            filename = f"{asset}_{tf}.json"
            url = GITHUB_API_URL + filename
            try:
                resp = requests.get(url, headers=headers)
                if resp.status_code == 200:
                    download_url = resp.json().get("download_url")
                    file_content = requests.get(download_url).json()
                    
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
                    
                    models_cache[asset][tf] = {
                        "strategies": strategies,
                        "expected_acc": file_content.get("combined_accuracy", 0),
                        "expected_trades": file_content.get("combined_trades", 0)
                    }
                    print(f"[{asset}] Loaded {tf} models.")
            except Exception as e:
                print(f"Error downloading {filename}: {e}")
    return models_cache

def get_binance_history(symbol, start_str=START_DATE, end_str=END_DATE):
    print(f"[{symbol}] Fetching full history ({start_str} to {end_str})...")
    start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int(datetime.strptime(end_str, "%Y-%m-%d").timestamp() * 1000)
    
    base_url = "https://api.binance.com/api/v3/klines"
    all_candles = []
    current_start = start_ts
    
    while current_start < end_ts:
        url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}&startTime={current_start}&endTime={end_ts}&limit=1000"
        try:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                if not data: break
                batch = [(int(c[6]), float(c[4])) for c in data]
                all_candles.extend(batch)
                last_time = data[-1][6]
                if last_time >= end_ts - 1000: break
                current_start = last_time + 1
        except Exception as e:
            print(f"[{symbol}] Error fetching history: {e}")
            break
    return all_candles

def get_binance_recent(symbol, days=20):
    """
    Fetches 20 days of data to ensure 1D models have enough history 
    (sequence length) to function, even if we only display 7 days.
    """
    end_ts = int(time.time() * 1000)
    start_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
    base_url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={BASE_INTERVAL}&endTime={end_ts}&limit=1000"
    all_candles = []
    current_start = start_ts
    
    while current_start < end_ts:
        url = f"{base_url}&startTime={current_start}"
        try:
            with urllib.request.urlopen(url) as r:
                data = json.loads(r.read().decode())
                if not data: break
                batch = [(int(c[6]), float(c[4])) for c in data]
                all_candles.extend(batch)
                last_time = data[-1][6]
                if last_time >= end_ts - 1000: break
                current_start = last_time + 1
        except:
            break
    return all_candles

# --- 3. INFERENCE LOGIC ---

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val, all_vals, all_changes):
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
    if not strategies: return 0
    active_directions = []
    
    max_seq_len = max(s['seq_len'] for s in strategies)
    if len(prices) < max_seq_len + 1: return 0

    for model in strategies:
        b_size = model['bucket_size']
        seq_len = model['seq_len']
        
        relevant_prices = prices[-seq_len:]
        buckets = [get_bucket(p, b_size) for p in relevant_prices]
        
        a_seq = tuple(buckets) 
        
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
    
    up = active_directions.count(1)
    down = active_directions.count(-1)
    
    if up > down: return 1
    elif down > up: return -1
    return 0

# --- 4. VERIFICATION & HISTORICAL ANALYSIS ---

def run_strict_verification(models_cache):
    print("\n========================================")
    print("STARTING STRICT MODEL VERIFICATION (2020-2026)")
    print("========================================")
    
    passed_all = True
    
    for asset in ASSETS:
        full_raw_data = get_binance_history(asset)
        
        for tf_name, model_data in models_cache[asset].items():
            strategies = model_data['strategies']
            exp_acc = model_data['expected_acc']
            exp_trades = model_data['expected_trades']
            
            tf_pandas = TIMEFRAMES[tf_name]
            prices = resample_prices(full_raw_data, tf_pandas)
            
            max_seq_len = max(s['seq_len'] for s in strategies)
            total_test_len = len(prices) - max_seq_len
            
            for s in strategies:
                s['cached_buckets'] = [get_bucket(p, s['bucket_size']) for p in prices]
            
            unique_correct = 0
            unique_total = 0
            
            for i in range(total_test_len):
                target_idx = i + max_seq_len
                active_directions = []
                
                for model in strategies:
                    seq_len = model['seq_len']
                    buckets = model['cached_buckets']
                    
                    start_seq_idx = target_idx - seq_len
                    a_seq = tuple(buckets[start_seq_idx : target_idx])
                    
                    if seq_len > 1:
                        d_seq = tuple(a_seq[k] - a_seq[k-1] for k in range(1, len(a_seq)))
                    else: d_seq = ()
                        
                    last_val = a_seq[-1]
                    actual_val = buckets[target_idx]
                    
                    diff = actual_val - last_val
                    model_actual_dir = 1 if diff > 0 else (-1 if diff < 0 else 0)
                    
                    pred_val = get_prediction(model['config']['model_type'], model['abs_map'], model['der_map'],
                                              a_seq, d_seq, last_val, model['all_vals'], model['all_changes'])
                    
                    pred_diff = pred_val - last_val
                    
                    if pred_diff != 0:
                        direction = 1 if pred_diff > 0 else -1
                        is_correct = (direction == model_actual_dir)
                        is_flat = (model_actual_dir == 0)
                        active_directions.append({"dir": direction, "is_correct": is_correct, "is_flat": is_flat})
                
                if not active_directions: continue
                
                dirs = [x['dir'] for x in active_directions]
                up_votes = dirs.count(1)
                down_votes = dirs.count(-1)
                
                final_dir = 0
                if up_votes > down_votes: final_dir = 1
                elif down_votes > up_votes: final_dir = -1
                else: continue
                
                winning_voters = [x for x in active_directions if x['dir'] == final_dir]
                if all(x['is_flat'] for x in winning_voters): continue
                
                unique_total += 1
                if any(x['is_correct'] for x in winning_voters): unique_correct += 1
            
            calc_acc = (unique_correct / unique_total * 100) if unique_total > 0 else 0
            acc_match = abs(calc_acc - exp_acc) < 0.5
            trade_match = abs(unique_total - exp_trades) < 5
            
            status = "PASS" if (acc_match and trade_match) else "FAIL"
            if status == "FAIL": passed_all = False
            print(f"[{status}] {asset} {tf_name} | Calc: {calc_acc:.2f}% ({unique_total}) | Json: {exp_acc:.2f}% ({exp_trades})")
            
    return passed_all

def populate_weekly_history(models_cache):
    """Iterates 20 days of data for calculation, but filters logs to last 7 days."""
    print("\n--- Generating Signal Log ---")
    global HISTORY_LOG
    logs = []
    
    cutoff_time = datetime.now() - timedelta(days=7)
    
    for asset in ASSETS:
        # Get 20 days raw 15m data (Safe buffer for 1D models)
        raw_data = get_binance_recent(asset, days=20)
        
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            # Use DataFrame to get actual timestamps
            df = get_resampled_df(raw_data, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            
            # Start enough steps in to allow for the longest sequence length
            max_seq = max(s['seq_len'] for s in strategies)
            start_idx = max_seq + 1
            
            # 1. LOOP RANGE FIX: Stop at len(prices) - 1 to ignore the forming candle
            if len(prices) <= start_idx: continue
            
            for i in range(start_idx, len(prices) - 1): 
                hist_prices = prices[:i] # Prices *before* the target timestamp
                current_price = prices[i-1] # The price at the moment of prediction
                target_ts = timestamps[i] # The "Future" time we are predicting for
                
                # 2. FILTER: Only add to log if within last 7 days
                if target_ts < cutoff_time:
                    continue
                
                # Signal generated at T-1 for target T
                sig = generate_signal(hist_prices, strategies)
                
                if sig != 0:
                    logs.append({
                        "time": target_ts.strftime("%Y-%m-%d %H:%M"),
                        "asset": asset,
                        "tf": tf_name,
                        "signal": "BUY" if sig == 1 else "SELL",
                        "price_at_signal": current_price
                    })
    
    # Sort by time descending (newest first)
    logs.sort(key=lambda x: x['time'], reverse=True)
    HISTORY_LOG = logs
    print(f"Generated {len(HISTORY_LOG)} historical signals.")

# --- 5. LIVE SERVER ---

def update_live_signals(models_cache):
    global SIGNAL_MATRIX, LAST_UPDATE
    print(f"\n[Scheduler] Updating signals at {datetime.now().strftime('%H:%M:%S')}...")
    temp_matrix = {}
    
    # Update Matrix
    for asset in ASSETS:
        temp_matrix[asset] = {}
        # Fetch 20 days so 1D models have enough history
        raw_data = get_binance_recent(asset, days=20)
        
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            prices = resample_prices(raw_data, tf_pandas)
            
            # CRITICAL FIX: Drop the last price bucket (forming candle).
            if prices:
                prices = prices[:-1]
            
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Refresh History occasionally
    populate_weekly_history(models_cache)
    
    print("[Scheduler] Signals Updated.")

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
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 30px; }}
            th, td {{ border: 1px solid #333; padding: 10px; text-align: center; }}
            th {{ background: #222; }}
            .buy {{ background: #004400; color: #0f0; }}
            .sell {{ background: #440000; color: #f00; }}
            .neutral {{ color: #555; }}
            h2 {{ border-bottom: 1px solid #444; padding-bottom: 5px; margin-top: 40px; }}
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
            row += f"<td class='{cls}'>{txt}</td>"
        row += "</tr>"
        html += row
        
    html += """
            </tbody>
        </table>
        
        <h2>Signals (Last 7 Days)</h2>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Asset</th>
                    <th>Timeframe</th>
                    <th>Signal</th>
                    <th>Price @ Sig</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for log in HISTORY_LOG[:100]: # Show last 100 signals
        cls = "buy" if log['signal'] == "BUY" else "sell"
        html += f"""
        <tr>
            <td>{log['time']}</td>
            <td>{log['asset']}</td>
            <td>{log['tf']}</td>
            <td class='{cls}'>{log['signal']}</td>
            <td>{log['price_at_signal']:.4f}</td>
        </tr>
        """
        
    html += "</tbody></table></body></html>"
    return render_template_string(html)

def run_scheduler(models_cache):
    print("Initializing Live Scheduler...")
    update_live_signals(models_cache)
    schedule.every().hour.at(":00").do(update_live_signals, models_cache)
    schedule.every().hour.at(":15").do(update_live_signals, models_cache)
    schedule.every().hour.at(":30").do(update_live_signals, models_cache)
    schedule.every().hour.at(":45").do(update_live_signals, models_cache)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # 1. Download Models
    models = fetch_models_from_github()
    
    # 2. Strict Verification
    if run_strict_verification(models):
        print("\n>>> VERIFICATION SUCCESSFUL. STARTING LIVE SERVER. <<<")
        
        # 3. Populate History Initial
        populate_weekly_history(models)
        
        # 4. Start Scheduler
        t = threading.Thread(target=run_scheduler, args=(models,))
        t.daemon = True
        t.start()
        
        # 5. Start Server
        app.run(host='0.0.0.0', port=8080)
    else:
        print("\n>>> VERIFICATION FAILED. SERVER WILL NOT START. <<<")
