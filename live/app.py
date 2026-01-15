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
from flask import Flask, render_template_string
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- DATABASE IMPORTS ---
try:
    from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
    from sqlalchemy.orm import sessionmaker, declarative_base
except ImportError:
    print("Warning: SQLAlchemy not found. DB features will be disabled.")
    class BaseDummy: metadata = type('obj', (object,), {'create_all': lambda x: None})
    declarative_base = lambda: BaseDummy

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

# Full Asset List
ASSETS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", 
    "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "TRXUSDT",
    "BCHUSDT", "XLMUSDT", "LTCUSDT", "SUIUSDT", "HBARUSDT",
    "SHIBUSDT", "TONUSDT", "UNIUSDT", "ZECUSDT", "BNBUSDT"
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

# --- GLOBAL STATE ---
SIGNAL_MATRIX = {}
HISTORY_LOG = []
HISTORY_ACCURACY = 0.0
LAST_UPDATE = "Never"

# HOT CACHE: Stores raw (timestamp, price) tuples
PRICE_CACHE = {asset: [] for asset in ASSETS}
CACHE_LOCK = threading.Lock()

# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL")
SessionLocal = None
Base = declarative_base()

if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    try:
        engine = create_engine(DATABASE_URL)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        print(">>> Database connection established.")
    except Exception as e:
        print(f">>> Database connection failed: {e}")
        DATABASE_URL = None

class HistoryEntry(Base):
    __tablename__ = 'signal_history'
    id = Column(Integer, primary_key=True, index=True)
    time_str = Column(String)
    asset = Column(String)
    tf = Column(String)
    signal = Column(String)
    price_at_signal = Column(Float)
    outcome = Column(String)
    close_price = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)

class MatrixEntry(Base):
    __tablename__ = 'live_matrix'
    asset = Column(String, primary_key=True)
    tf = Column(String, primary_key=True)
    signal_val = Column(Integer)
    updated_at = Column(DateTime, default=datetime.utcnow)

if DATABASE_URL and engine:
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f">>> Error creating tables: {e}")

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

# --- 2. DATA FETCHING ---

def fetch_binance_candles(symbol, limit=1000, start_time=None):
    base_url = "https://api.binance.com/api/v3/klines"
    url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}"
    
    if start_time:
        url += f"&startTime={start_time}&limit=1000"
    else:
        url += f"&limit={limit}"
        
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())
            if not data: return []
            return [(int(c[6]), float(c[4])) for c in data]
    except Exception as e:
        print(f"[{symbol}] Fetch Error: {e}")
        return []

def update_asset_cache_live(asset):
    global PRICE_CACHE
    recent_candles = fetch_binance_candles(asset, limit=5)
    if not recent_candles: return

    with CACHE_LOCK:
        current_data = PRICE_CACHE.get(asset, [])
        data_dict = dict(current_data)
        for ts, price in recent_candles:
            data_dict[ts] = price
        sorted_items = sorted(data_dict.items())
        if len(sorted_items) > 2500: sorted_items = sorted_items[-2500:]
        PRICE_CACHE[asset] = sorted_items

# --- 3. GITHUB SYNC (FULL) ---

def sync_ohlc_full(symbol):
    print(f"[{symbol}] Loading full history...")
    filename = f"ohlc/{symbol}.csv"
    url = GITHUB_API_BASE + filename
    headers = {"Authorization": f"Bearer {GITHUB_PAT}"} if GITHUB_PAT else {}
    
    existing_data = []
    last_stored_ts = 0
    
    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            file_info = resp.json()
            download_url = file_info.get("download_url")
            content_resp = requests.get(download_url)
            if content_resp.status_code == 200:
                csv_text = content_resp.text
                df_exist = pd.read_csv(StringIO(csv_text), names=["timestamp", "price"])
                if not df_exist.empty:
                    existing_data = list(df_exist.itertuples(index=False, name=None))
                    last_stored_ts = int(existing_data[-1][0])
    except Exception: pass

    if last_stored_ts == 0:
        start_ts = int(datetime.strptime(START_DATE, "%Y-%m-%d").timestamp() * 1000)
    else:
        start_ts = last_stored_ts + 1
        
    end_ts = int(datetime.strptime(END_DATE, "%Y-%m-%d").timestamp() * 1000)
    current_ts = int(time.time() * 1000)
    if end_ts > current_ts: end_ts = current_ts

    new_data = []
    curr = start_ts
    while curr < end_ts:
        batch = fetch_binance_candles(symbol, start_time=curr)
        if not batch: break
        new_data.extend(batch)
        curr = batch[-1][0] + 1
        time.sleep(0.05)
        if len(batch) < 500: break

    full_data = existing_data + new_data
    return full_data

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
            except Exception as e:
                print(f"Error downloading {filename}: {e}")
    return models_cache

# --- 4. INFERENCE ---

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val, all_vals, all_changes):
    if model_type == "Absolute":
        if a_seq in abs_map: 
            return abs_map[a_seq].most_common(1)[0][0]
    elif model_type == "Derivative":
        if d_seq in der_map: 
            pred_change = der_map[d_seq].most_common(1)[0][0]
            return last_val + pred_change
    elif model_type == "Combined":
        abs_cand = abs_map.get(a_seq, Counter())
        der_cand = der_map.get(d_seq, Counter())
        poss = set(abs_cand.keys())
        for c in der_cand.keys(): poss.add(last_val + c)
        if not poss: return None
        best, max_s = None, -1
        for v in poss:
            s = abs_cand[v] + der_cand[v - last_val]
            if s > max_s: max_s, best = s, v
        return best
    return None

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
        else: d_seq = ()
            
        last_val = a_seq[-1]
        
        pred_val = get_prediction(
            model['config']['model_type'], 
            model['abs_map'], model['der_map'], 
            a_seq, d_seq, last_val, 
            model['all_vals'], model['all_changes']
        )
        
        if pred_val is None: continue

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

# --- 5. VERIFICATION & HISTORY ---

def run_strict_verification(models_cache):
    print("\n========================================")
    print("STARTING STRICT MODEL VERIFICATION")
    print("========================================")
    passed_all = True
    
    def verify_asset(asset):
        full_raw_data = sync_ohlc_full(asset)
        if not full_raw_data:
            print(f"[FAIL] {asset} - No Data")
            return False

        asset_passed = True
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
                    
                    pred_val = get_prediction(model['config']['model_type'], model['abs_map'], model['der_map'],
                                              a_seq, d_seq, last_val, model['all_vals'], model['all_changes'])
                    
                    if pred_val is None: continue

                    pred_diff = pred_val - last_val
                    
                    if pred_diff != 0:
                        direction = 1 if pred_diff > 0 else -1
                        actual_diff = actual_val - last_val
                        model_actual_dir = 1 if actual_diff > 0 else (-1 if actual_diff < 0 else 0)
                        
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
            print(f"[{status}] {asset} {tf_name} | Calc: {calc_acc:.2f}% ({unique_total}) | Json: {exp_acc:.2f}% ({exp_trades})")
            if status == "FAIL": asset_passed = False
            
        if asset_passed:
            with CACHE_LOCK:
                PRICE_CACHE[asset] = full_raw_data[-2500:] 
        
        return asset_passed

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(verify_asset, ASSETS))
        
    if not all(results):
        passed_all = False

    return passed_all

def populate_weekly_history(models_cache):
    global HISTORY_LOG, HISTORY_ACCURACY
    logs = []
    
    # Use UTC to align with Binance
    cutoff_time = datetime.utcnow() - timedelta(days=7)
    print(f"\n[HISTORY DEBUG] Cutoff: {cutoff_time} (UTC)")
    
    total_wins = 0
    total_valid_moves = 0
    
    for asset in ASSETS:
        with CACHE_LOCK:
            raw_data = list(PRICE_CACHE[asset]) 
        
        # DEBUG: Check first asset's range
        if len(raw_data) > 0 and asset == "BTCUSDT":
             first_ts = datetime.utcfromtimestamp(raw_data[0][0]/1000)
             last_ts = datetime.utcfromtimestamp(raw_data[-1][0]/1000)
             print(f"[HISTORY DEBUG] BTCUSDT Data Range: {first_ts} -> {last_ts}")

        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            if not strategies: continue

            tf_pandas = TIMEFRAMES[tf_name]
            min_bucket_size = min(s['bucket_size'] for s in strategies)
            
            df = get_resampled_df(raw_data, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            
            max_seq = max(s['seq_len'] for s in strategies)
            start_idx = max_seq + 1
            if len(prices) <= start_idx: continue
            
            # Counter for how many checks we actually did
            check_count = 0
            
            for i in range(start_idx, len(prices) - 1): 
                target_ts = timestamps[i]
                
                # DATE FILTER DEBUG
                if target_ts < cutoff_time: 
                    continue
                
                check_count += 1
                hist_prices = prices[:i]
                current_price = prices[i-1]
                outcome_price = prices[i]
                
                sig = generate_signal(hist_prices, strategies)
                
                # LOGGING ONE SAMPLE FOR DEBUGGING
                if check_count == 1 and asset == "BTCUSDT":
                    print(f"[HISTORY DEBUG] BTCUSDT First Check @ {target_ts}: Sig={sig}, Price={current_price}")

                if sig != 0:
                    start_bucket = get_bucket(current_price, min_bucket_size)
                    end_bucket = get_bucket(outcome_price, min_bucket_size)
                    bucket_diff = end_bucket - start_bucket
                    
                    outcome_str = "NOISE"
                    if sig == 1:
                        if bucket_diff > 0: outcome_str = "WIN"
                        elif bucket_diff < 0: outcome_str = "LOSS"
                    elif sig == -1:
                        if bucket_diff < 0: outcome_str = "WIN"
                        elif bucket_diff > 0: outcome_str = "LOSS"
                    
                    if outcome_str == "WIN":
                        total_wins += 1; total_valid_moves += 1
                    elif outcome_str == "LOSS":
                        total_valid_moves += 1
                    
                    pct_change = ((outcome_price - current_price) / current_price) * 100

                    logs.append({
                        "time": target_ts.strftime("%Y-%m-%d %H:%M"),
                        "asset": asset,
                        "tf": tf_name,
                        "signal": "BUY" if sig == 1 else "SELL",
                        "price_at_signal": current_price,
                        "outcome": outcome_str,
                        "close_price": outcome_price,
                        "pct_change": pct_change
                    })
    
    logs.sort(key=lambda x: x['time'], reverse=True)
    HISTORY_LOG = logs
    HISTORY_ACCURACY = (total_wins / total_valid_moves * 100) if total_valid_moves > 0 else 0.0
    
    print(f"Generated {len(HISTORY_LOG)} signals. Strict Accuracy: {HISTORY_ACCURACY:.2f}%")

    if SessionLocal:
        try:
            session = SessionLocal()
            session.query(HistoryEntry).delete()
            for log in logs:
                entry = HistoryEntry(
                    time_str=log['time'],
                    asset=log['asset'],
                    tf=log['tf'],
                    signal=log['signal'],
                    price_at_signal=log['price_at_signal'],
                    outcome=log['outcome'],
                    close_price=log['close_price']
                )
                session.add(entry)
            session.commit()
            session.close()
        except Exception: pass

# --- 6. LIVE SERVER ---

def update_live_signals(models_cache):
    global SIGNAL_MATRIX, LAST_UPDATE
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(update_asset_cache_live, asset): asset for asset in ASSETS}
        for future in as_completed(futures): pass
            
    temp_matrix = {}
    for asset in ASSETS:
        temp_matrix[asset] = {}
        with CACHE_LOCK:
            raw_data = list(PRICE_CACHE[asset])
            
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            prices = resample_prices(raw_data, tf_pandas)
            
            if prices: prices = prices[:-1]
            
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    if SessionLocal:
        try:
            session = SessionLocal()
            session.query(MatrixEntry).delete()
            for asset, tfs in temp_matrix.items():
                for tf, val in tfs.items():
                    session.add(MatrixEntry(asset=asset, tf=tf, signal_val=val))
            session.commit()
            session.close()
        except Exception: pass
    
    populate_weekly_history(models_cache)
    
    duration = time.time() - start_time
    print(f"[Scheduler] Update Complete in {duration:.2f}s.")

app = Flask(__name__)

@app.route('/')
def home():
    acc_color = "#0f0" if HISTORY_ACCURACY > 50 else "#f00"
    html = f"""
    <html>
    <head>
        <title>Strategy Matrix</title>
        <meta http-equiv="refresh" content="5">
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
        <p>Last Update: {LAST_UPDATE} (UTC)</p>
        
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
                    <th>Time (UTC)</th>
                    <th>Asset</th>
                    <th>Timeframe</th>
                    <th>Signal</th>
                    <th>Price @ Sig</th>
                    <th>% Change</th>
                    <th>Outcome</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for log in HISTORY_LOG[:150]:
        cls = "buy" if log['signal'] == "BUY" else "sell"
        res_cls = log['outcome']
        pct_txt = f"{log['pct_change']:+.2f}%"
        
        html += f"""
        <tr>
            <td>{log['time']}</td>
            <td>{log['asset']}</td>
            <td>{log['tf']}</td>
            <td class='{cls}'>{log['signal']}</td>
            <td>{log['price_at_signal']:.4f}</td>
            <td>{pct_txt}</td>
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
        print("\n>>> VERIFICATION SUCCESSFUL. STARTING LIVE SERVER (FAST MODE). <<<")
        t = threading.Thread(target=run_scheduler, args=(models,))
        t.daemon = True
        t.start()
        app.run(host='0.0.0.0', port=8080)
    else:
        print("\n>>> VERIFICATION FAILED. SERVER WILL NOT START. <<<")
