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
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- DATABASE IMPORTS ---
try:
    from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
    from sqlalchemy.orm import sessionmaker, declarative_base
    from sqlalchemy.sql import text
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

ASSETS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", 
    "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "TRXUSDT",
    "BCHUSDT", "XLMUSDT", "LTCUSDT", "SUIUSDT", "HBARUSDT",
    "SHIBUSDT", "TONUSDT", "UNIUSDT", "ZECUSDT", "BNBUSDT"
]

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

# In-Memory Data Cache (The "Stateful" Part)
# Format: { "BTCUSDT": pd.DataFrame(columns=['timestamp', 'price']), ... }
DATA_CACHE = {} 
CACHE_DAYS = 15  # Buffer to cover 10d sequence + resampling overhead

# --- 0. DATABASE SETUP ---
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

def get_resampled_df(df, target_freq):
    """
    Expects a DataFrame with a datetime index.
    Returns a DataFrame with 'price' column.
    """
    if df.empty: return pd.DataFrame()
    if target_freq is None: 
        return df[['price']]
    
    # Resample and take the last close
    resampled = df['price'].resample(target_freq).last().dropna().to_frame()
    return resampled

# --- 2. DATA MANAGEMENT (OPTIMIZED) ---

def fetch_binance_candles(symbol, limit=1000):
    """
    Fetches the latest N candles. 
    Used for both initialization (limit=1000) and live updates (limit=5).
    """
    base_url = "https://api.binance.com/api/v3/klines"
    url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}&limit={limit}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            if not data: return []
            # Parse: [timestamp(ms), open, high, low, close, ...]
            # We only need Close Price and Close Time
            return [(int(c[6]), float(c[4])) for c in data]
    except Exception as e:
        print(f"[{symbol}] Error fetching: {e}")
        return []

def initialize_cache():
    print(f"--- Initializing Data Cache ({CACHE_DAYS} Days) ---")
    
    def load_asset(asset):
        # Calculate how many 15m candles in X days
        # 24 * 4 = 96 candles per day. 
        limit = CACHE_DAYS * 96 
        if limit > 1000: limit = 1000 # Binance max per request is 1000
        
        # If we really need >1000, we'd need loop, but for now 1000 is ~10 days. 
        # To get 15 days, we might need 2 calls, but let's start with 1000 for speed.
        raw = fetch_binance_candles(asset, limit=1000)
        
        df = pd.DataFrame(raw, columns=['timestamp', 'price'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return asset, df

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(load_asset, asset) for asset in ASSETS]
        for future in as_completed(futures):
            asset, df = future.result()
            DATA_CACHE[asset] = df
            print(f"[{asset}] Cache initialized with {len(df)} candles.")

def update_cache_parallel():
    """
    Fast update: Fetches last 5 candles for ALL assets in parallel.
    Merges into global cache.
    """
    def fetch_update(asset):
        # Fetch just enough to cover potential gaps
        new_data = fetch_binance_candles(asset, limit=5)
        return asset, new_data

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_update, asset) for asset in ASSETS]
        for future in as_completed(futures):
            asset, raw_data = future.result()
            if not raw_data: continue
            
            new_df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms')
            new_df.set_index('timestamp', inplace=True)
            
            # Combine and deduplicate
            current_df = DATA_CACHE.get(asset, pd.DataFrame())
            combined = pd.concat([current_df, new_df])
            combined = combined[~combined.index.duplicated(keep='last')]
            
            # Keep size manageable (last 1500 rows ~ 15 days)
            if len(combined) > 1500:
                combined = combined.iloc[-1500:]
            
            DATA_CACHE[asset] = combined

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
                        "strategies": strategies
                    }
            except Exception as e:
                pass # Silent fail to keep logs clean
    print(">>> Models Loaded.")
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
    if not prices: return 0
    
    active_directions = []
    max_seq_len = max(s['seq_len'] for s in strategies)
    
    # Check strict length requirement
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

# --- 4. BACKGROUND TASKS (SLOW PATH) ---

def run_history_analysis(models_cache):
    """
    Runs complex historical analysis and GitHub syncing.
    This is slow, so it runs in a background thread.
    """
    print("[Background] Starting History Analysis...")
    global HISTORY_LOG, HISTORY_ACCURACY
    logs = []
    
    # Use data from cache directly to avoid fetching again
    cutoff_time = datetime.now() - timedelta(days=7)
    total_wins = 0
    total_valid_moves = 0
    
    for asset in ASSETS:
        df_master = DATA_CACHE.get(asset)
        if df_master is None or df_master.empty: continue

        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            min_bucket_size = min(s['bucket_size'] for s in strategies)
            
            # Resample from cached master data
            df = get_resampled_df(df_master, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            max_seq = max(s['seq_len'] for s in strategies)
            start_idx = max_seq + 1
            
            if len(prices) <= start_idx: continue
            
            # Analyze last 100 points only to save CPU
            scan_start = max(start_idx, len(prices) - 100) 
            
            for i in range(scan_start, len(prices) - 1): 
                target_ts = timestamps[i]
                if target_ts < cutoff_time: continue

                hist_prices = prices[:i]
                current_price = prices[i-1]
                outcome_price = prices[i]
                
                sig = generate_signal(hist_prices, strategies)
                
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
    print(f"[Background] History Updated. Acc: {HISTORY_ACCURACY:.2f}%")

    # Save History to DB
    if SessionLocal:
        try:
            session = SessionLocal()
            session.query(HistoryEntry).delete()
            for log in logs:
                entry = HistoryEntry(
                    time_str=log['time'], asset=log['asset'], tf=log['tf'],
                    signal=log['signal'], price_at_signal=log['price_at_signal'],
                    outcome=log['outcome'], close_price=log['close_price']
                )
                session.add(entry)
            session.commit()
            session.close()
        except Exception as e:
            print(f"[DB Error] History save failed: {e}")

# --- 5. LIVE SERVER (FAST PATH) ---

def save_matrix_to_db(matrix):
    """
    Fast DB commit for the live matrix only.
    """
    if not SessionLocal: return
    try:
        session = SessionLocal()
        session.query(MatrixEntry).delete()
        for asset, tfs in matrix.items():
            for tf, val in tfs.items():
                session.add(MatrixEntry(asset=asset, tf=tf, signal_val=val))
        session.commit()
        session.close()
        print("[DB] Matrix saved.")
    except Exception as e:
        print(f"[DB Error] Matrix save failed: {e}")

def update_live_signals(models_cache):
    """
    CRITICAL PATH: Must run < 5 seconds.
    1. Parallel Fetch (Network)
    2. In-Memory Stitch (CPU)
    3. Generate Signals (CPU)
    4. Save Matrix (DB)
    5. Spawn Background History (Thread)
    """
    global SIGNAL_MATRIX, LAST_UPDATE
    start_time = time.time()
    print(f"\n[Scheduler] Start Update: {datetime.now().strftime('%H:%M:%S')}")
    
    # 1. Update Data Cache (Parallel)
    update_cache_parallel()
    
    temp_matrix = {}
    
    # 2 & 3. Compute Signals
    for asset in ASSETS:
        temp_matrix[asset] = {}
        df_master = DATA_CACHE.get(asset)
        
        if df_master is None or df_master.empty:
            continue
            
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            # Fast in-memory resample
            df_resampled = get_resampled_df(df_master, tf_pandas)
            
            # Convert to list for legacy logic
            # We strip the last candle to match the "completed candle" logic
            prices = df_resampled['price'].tolist()[:-1] 
            
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 4. Save to DB (Blocking but fast)
    save_matrix_to_db(temp_matrix)
    
    elapsed = time.time() - start_time
    print(f"[Scheduler] Signals Generated in {elapsed:.2f}s")
    
    # 5. Background Tasks
    t = threading.Thread(target=run_history_analysis, args=(models_cache,))
    t.start()

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
                    <th>% Change</th>
                    <th>Outcome</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for log in HISTORY_LOG[:100]:
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
    # Initialize Cache First
    initialize_cache()
    
    # Initial Run
    update_live_signals(models_cache)
    
    # Schedule
    schedule.every().hour.at(":00").do(update_live_signals, models_cache)
    schedule.every().hour.at(":15").do(update_live_signals, models_cache)
    schedule.every().hour.at(":30").do(update_live_signals, models_cache)
    schedule.every().hour.at(":45").do(update_live_signals, models_cache)
    
    while True:
        schedule.run_pending()
        time.sleep(0.5)

if __name__ == "__main__":
    models = fetch_models_from_github()
    print(">>> Starting Server...")
    t = threading.Thread(target=run_scheduler, args=(models,))
    t.daemon = True
    t.start()
    app.run(host='0.0.0.0', port=8080)
