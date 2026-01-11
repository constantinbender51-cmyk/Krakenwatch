import os
import json
import time
import requests
import threading
import schedule
import pandas as pd
import urllib.request
import base64
from io import StringIO
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
GITHUB_API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

ASSETS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", 
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT"
]

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
HISTORY_LOG = []
HISTORY_ACCURACY = 0.0
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
    if not raw_data: return pd.DataFrame()
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    if target_freq is None: return df
    return df['price'].resample(target_freq).last().dropna().to_frame()

# --- 2. DATA MANAGEMENT ---

def fetch_binance_segment(symbol, start_ts, end_ts):
    base_url = "https://api.binance.com/api/v3/klines"
    segment_candles = []
    current_start = start_ts
    while current_start < end_ts:
        url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}&startTime={current_start}&endTime={end_ts}&limit=1000"
        try:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                if not data: break
                batch = [(int(c[6]), float(c[4])) for c in data]
                segment_candles.extend(batch)
                last_time = data[-1][6]
                if last_time >= end_ts - 1000: break
                current_start = last_time + 1
        except Exception as e:
            print(f"[{symbol}] Error fetching segment: {e}")
            break
    return segment_candles

def sync_ohlc_with_github(symbol):
    print(f"[{symbol}] Syncing OHLC data...")
    filename = f"ohlc/{symbol}.csv"
    url = GITHUB_API_BASE + filename
    headers = {"Authorization": f"Bearer {GITHUB_PAT}"} if GITHUB_PAT else {}
    
    existing_data = []
    sha = None
    last_stored_ts = 0
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            file_info = resp.json()
            sha = file_info.get("sha")
            download_url = file_info.get("download_url")
            content_resp = requests.get(download_url)
            if content_resp.status_code == 200:
                csv_text = content_resp.text
                df_exist = pd.read_csv(StringIO(csv_text), names=["timestamp", "price"])
                if not df_exist.empty:
                    existing_data = list(df_exist.itertuples(index=False, name=None))
                    last_stored_ts = int(existing_data[-1][0])
                    print(f"[{symbol}] Found cached data up to {datetime.fromtimestamp(last_stored_ts/1000)}")
    except Exception as e:
        print(f"[{symbol}] Could not fetch existing data: {e}")

    if last_stored_ts == 0:
        start_ts = int(datetime.strptime(START_DATE, "%Y-%m-%d").timestamp() * 1000)
    else:
        start_ts = last_stored_ts + 1
        
    end_ts = int(datetime.strptime(END_DATE, "%Y-%m-%d").timestamp() * 1000)
    current_ts = int(time.time() * 1000)
    if end_ts > current_ts: end_ts = current_ts

    new_data = []
    if start_ts < end_ts:
        print(f"[{symbol}] Fetching new data from Binance...")
        new_data = fetch_binance_segment(symbol, start_ts, end_ts)
        print(f"[{symbol}] Fetched {len(new_data)} new candles.")
    
    full_data = existing_data + new_data
    
    if new_data and full_data:
        try:
            df_full = pd.DataFrame(full_data, columns=["timestamp", "price"])
            csv_buffer = StringIO()
            df_full.to_csv(csv_buffer, header=False, index=False)
            csv_content = csv_buffer.getvalue()
            b64_content = base64.b64encode(csv_content.encode('utf-8')).decode('utf-8')
            payload = {
                "message": f"Update {symbol} OHLC data",
                "content": b64_content,
                "branch": "main"
            }
            if sha: payload["sha"] = sha
            requests.put(url, headers=headers, json=payload)
            print(f"[{symbol}] Saved updated OHLC to GitHub.")
        except Exception as e:
            print(f"[{symbol}] Error saving to GitHub: {e}")

    return full_data

def get_binance_recent(symbol, days=20):
    end_ts = int(time.time() * 1000)
    start_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    return fetch_binance_segment(symbol, start_ts, end_ts)

def fetch_models_from_github():
    print("--- Downloading Strategies from GitHub ---")
    headers = {"Authorization": f"Bearer {GITHUB_PAT}"} if GITHUB_PAT else {}
    models_cache = {}
    
    for asset in ASSETS:
        models_cache[asset] = {}
        for tf in TIMEFRAMES.keys():
            filename = f"{asset}_{tf}.json"
            url = GITHUB_API_BASE + filename
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

# --- 3. INFERENCE LOGIC ---

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val, all_vals, all_changes):
    if model_type == "Absolute":
        if a_seq in abs_map: 
            return abs_map[a_seq].most_common(1)[0][0]
        else:
            return last_val
    elif model_type == "Derivative":
        if d_seq in der_map: 
            pred_change = der_map[d_seq].most_common(1)[0][0]
            return last_val + pred_change
        else:
            return last_val
    elif model_type == "Combined":
        abs_cand = abs_map.get(a_seq, Counter())
        der_cand = der_map.get(d_seq, Counter())
        poss = set(abs_cand.keys())
        for c in der_cand.keys(): poss.add(last_val + c)
        if not poss: return last_val
        best, max_s = None, -1
        for v in poss:
            s = abs_cand[v] + der_cand[v - last_val]
            if s > max_s: max_s, best = s, v
        return best if best is not None else last_val
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
    print("STARTING STRICT MODEL VERIFICATION")
    print("========================================")
    passed_all = True
    
    for asset in ASSETS:
        full_raw_data = sync_ohlc_with_github(asset)
        
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
    print("\n--- Generating Signal Log & Calculating Strict Accuracy ---")
    global HISTORY_LOG, HISTORY_ACCURACY
    logs = []
    
    cutoff_time = datetime.now() - timedelta(days=7)
    
    total_wins = 0
    total_valid_moves = 0
    
    for asset in ASSETS:
        raw_data = get_binance_recent(asset, days=20)
        
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            # Identify the smallest bucket size used in this timeframe (High Sensitivity)
            # We use this to determine if the price moved "enough"
            min_bucket_size = min(s['bucket_size'] for s in strategies)
            
            df = get_resampled_df(raw_data, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            
            max_seq = max(s['seq_len'] for s in strategies)
            start_idx = max_seq + 1
            
            if len(prices) <= start_idx: continue
            
            for i in range(start_idx, len(prices) - 1): 
                hist_prices = prices[:i]
                current_price = prices[i-1]
                target_ts = timestamps[i]
                
                outcome_price = prices[i]
                
                if target_ts < cutoff_time:
                    continue
                
                sig = generate_signal(hist_prices, strategies)
                
                if sig != 0:
                    # STRICT VERIFICATION: Did it move a full bucket?
                    start_bucket = get_bucket(current_price, min_bucket_size)
                    end_bucket = get_bucket(outcome_price, min_bucket_size)
                    bucket_diff = end_bucket - start_bucket
                    
                    outcome_str = "NOISE"
                    
                    if sig == 1: # Predicted UP
                        if bucket_diff > 0: outcome_str = "WIN"
                        elif bucket_diff < 0: outcome_str = "LOSS"
                        # else bucket_diff == 0 -> NOISE
                        
                    elif sig == -1: # Predicted DOWN
                        if bucket_diff < 0: outcome_str = "WIN"
                        elif bucket_diff > 0: outcome_str = "LOSS"
                        # else bucket_diff == 0 -> NOISE
                    
                    # Update Stats (Exclude Noise from Accuracy)
                    if outcome_str == "WIN":
                        total_wins += 1
                        total_valid_moves += 1
                    elif outcome_str == "LOSS":
                        total_valid_moves += 1
                    
                    logs.append({
                        "time": target_ts.strftime("%Y-%m-%d %H:%M"),
                        "asset": asset,
                        "tf": tf_name,
                        "signal": "BUY" if sig == 1 else "SELL",
                        "price_at_signal": current_price,
                        "outcome": outcome_str,
                        "close_price": outcome_price
                    })
    
    logs.sort(key=lambda x: x['time'], reverse=True)
    HISTORY_LOG = logs
    
    if total_valid_moves > 0:
        HISTORY_ACCURACY = (total_wins / total_valid_moves) * 100
    else:
        HISTORY_ACCURACY = 0.0
        
    print(f"Generated {len(HISTORY_LOG)} signals. Strict Accuracy: {HISTORY_ACCURACY:.2f}% (Moves: {total_valid_moves})")

# --- 5. LIVE SERVER ---

def update_live_signals(models_cache):
    global SIGNAL_MATRIX, LAST_UPDATE
    print(f"\n[Scheduler] Updating signals at {datetime.now().strftime('%H:%M:%S')}...")
    temp_matrix = {}
    
    for asset in ASSETS:
        temp_matrix[asset] = {}
        raw_data = get_binance_recent(asset, days=20)
        
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            prices = resample_prices(raw_data, tf_pandas)
            
            if prices:
                prices = prices[:-1]
            
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    populate_weekly_history(models_cache)
    print("[Scheduler] Signals Updated.")

app = Flask(__name__)

@app.route('/')
def home():
    acc_color = "#0f0" if HISTORY_ACCURACY > 50 else "#f00"
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
            .WIN {{ color: #0f0; font-weight: bold; }}
            .LOSS {{ color: #f00; font-weight: bold; }}
            .NOISE {{ color: #666; font-style: italic; }}
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
        
    html += f"""
            </tbody>
        </table>
        
        <h2>Signals (Last 7 Days)</h2>
        <p>Strict Accuracy (Excluding Noise): <span style="color:{acc_color}; font-size: 1.2em;">{HISTORY_ACCURACY:.2f}%</span></p>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Asset</th>
                    <th>Timeframe</th>
                    <th>Signal</th>
                    <th>Price @ Sig</th>
                    <th>Outcome</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for log in HISTORY_LOG[:150]:
        cls = "buy" if log['signal'] == "BUY" else "sell"
        res_cls = log['outcome'] # WIN, LOSS, NOISE
        html += f"""
        <tr>
            <td>{log['time']}</td>
            <td>{log['asset']}</td>
            <td>{log['tf']}</td>
            <td class='{cls}'>{log['signal']}</td>
            <td>{log['price_at_signal']:.4f}</td>
            <td class='{res_cls}'>{log['outcome']}</td>
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
    models = fetch_models_from_github()
    if run_strict_verification(models):
        print("\n>>> VERIFICATION SUCCESSFUL. STARTING LIVE SERVER. <<<")
        populate_weekly_history(models)
        t = threading.Thread(target=run_scheduler, args=(models,))
        t.daemon = True
        t.start()
        app.run(host='0.0.0.0', port=8080)
    else:
        print("\n>>> VERIFICATION FAILED. SERVER WILL NOT START. <<<")
