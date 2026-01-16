import os
import json
import time
import requests
import threading
import schedule
import pandas as pd
import urllib.request
import base64
import random
from io import StringIO
from datetime import datetime, timedelta
from flask import Flask, render_template_string

# =========================================
# 0. CONFIGURATION & SETUP
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- DATABASE IMPORTS ---
try:
    from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
    from sqlalchemy.orm import sessionmaker, declarative_base
except ImportError:
    print("Warning: SQLAlchemy not found. DB features will be disabled.")
    # Dummy classes to prevent crash if libs missing
    class BaseDummy: metadata = type('obj', (object,), {'create_all': lambda x: None})
    declarative_base = lambda: BaseDummy

# --- GITHUB SETTINGS ---
GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"
GITHUB_API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# --- ASSETS ---
ASSETS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", 
    "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "TRXUSDT",
    "BCHUSDT", "XLMUSDT", "LTCUSDT", "SUIUSDT", "HBARUSDT",
    "SHIBUSDT", "TONUSDT", "UNIUSDT", "ZECUSDT"
]

# --- DATA SETTINGS ---
BASE_INTERVAL = "1m" # Must match training script
TIMEFRAMES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H"
}

# --- GLOBAL STATE ---
SIGNAL_MATRIX = {}
HISTORY_LOG = []
HISTORY_ACCURACY = 0.0
TOTAL_PNL = 0.0
LAST_UPDATE = "Never"
PRICE_CACHE = {} 

# =========================================
# 1. DATABASE MODELS
# =========================================

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
    __tablename__ = 'signal_history_v2'
    id = Column(Integer, primary_key=True, index=True)
    time_str = Column(String) 
    asset = Column(String)
    tf = Column(String)
    signal = Column(String)
    price_at_signal = Column(Float)
    outcome = Column(String)
    close_price = Column(Float)
    pnl = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)

class MatrixEntry(Base):
    __tablename__ = 'live_matrix_v2'
    asset = Column(String, primary_key=True)
    tf = Column(String, primary_key=True)
    signal_val = Column(Integer)
    updated_at = Column(DateTime, default=datetime.utcnow)

if DATABASE_URL and engine:
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f">>> Error creating tables: {e}")

# =========================================
# 2. UTILITIES
# =========================================

def get_bucket(price, bucket_size):
    if bucket_size <= 0: bucket_size = 1e-9
    return int(price // bucket_size)

def make_key(seq):
    return "|".join(map(str, seq))

def resample_prices(raw_data, target_freq):
    """
    Takes raw 1m data (list of floats) and resamples to target timeframe.
    """
    if not raw_data: return []
    
    # Optimization: If target is 1m, no pandas overhead needed
    if target_freq == "1min" or target_freq == "1m":
         return [x[1] for x in raw_data]
         
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Resample using 'last' to get close prices of the larger candles
    resampled = df['price'].resample(target_freq).last().dropna()
    return resampled.tolist()

def get_resampled_df(raw_data, target_freq):
    """
    Returns DataFrame with timestamps for history generation.
    """
    if not raw_data: return pd.DataFrame()
    df = pd.DataFrame(raw_data, columns=['timestamp', 'price'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    if target_freq == "1min" or target_freq == "1m":
        return df[['price']]
        
    return df['price'].resample(target_freq).last().dropna().to_frame()

# =========================================
# 3. DATA MANAGEMENT
# =========================================

def fetch_binance_segment(symbol, start_ts, end_ts):
    base_url = "https://api.binance.com/api/v3/klines"
    segment_candles = []
    current_start = start_ts
    
    while current_start < end_ts:
        # ALWAYS fetch 1m candles. This allows us to build any other timeframe.
        url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}&startTime={current_start}&endTime={end_ts}&limit=1000"
        try:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                if not data: break
                
                # Store [timestamp, close_price]
                batch = [(int(c[0]), float(c[4])) for c in data]
                segment_candles.extend(batch)
                
                last_time = data[-1][0]
                if last_time >= end_ts - 1000: break
                current_start = last_time + 1
        except Exception as e:
            print(f"[{symbol}] Error fetching segment: {e}")
            break
            
    return segment_candles

def get_binance_recent(symbol, days=5):
    """Fetches a large chunk of history for initialization."""
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
                    raw_strategies = file_content.get("strategies", [])
                    
                    for s in raw_strategies:
                        cfg = s.get("config", {})
                        params = s.get("params", {})
                        
                        strategies.append({
                            "b_count": cfg.get("b_count"),
                            "s_len": cfg.get("s_len"),
                            "model_type": cfg.get("model"),
                            "bucket_size": params.get("bucket_size"),
                            "abs_map": params.get("abs_map", {}),
                            "der_map": params.get("der_map", {})
                        })
                    
                    stats = file_content.get("holdout_stats", {})
                    models_cache[asset][tf] = {
                        "strategies": strategies,
                        "expected_acc": stats.get("accuracy", 0),
                        "expected_trades": stats.get("trades", 0)
                    }
                    print(f"[{asset}] Loaded {tf} models")
            except Exception:
                pass 
                
    return models_cache

# =========================================
# 4. INFERENCE LOGIC
# =========================================

def get_single_prediction(mode, abs_map, der_map, a_seq, d_seq, last_val):
    if mode == "Absolute":
        key = make_key(a_seq)
        if key in abs_map:
            return abs_map[key]
            
    elif mode == "Derivative":
        key = make_key(d_seq)
        if key in der_map:
            pred_change = der_map[key]
            return last_val + pred_change
            
    return None

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val):
    if model_type == "Absolute":
        return get_single_prediction("Absolute", abs_map, der_map, a_seq, d_seq, last_val)
        
    elif model_type == "Derivative":
        return get_single_prediction("Derivative", abs_map, der_map, a_seq, d_seq, last_val)
        
    elif model_type == "Combined":
        pred_abs = get_single_prediction("Absolute", abs_map, der_map, a_seq, d_seq, last_val)
        pred_der = get_single_prediction("Derivative", abs_map, der_map, a_seq, d_seq, last_val)
        
        dir_abs = 0
        if pred_abs is not None:
            dir_abs = 1 if pred_abs > last_val else -1 if pred_abs < last_val else 0
            
        dir_der = 0
        if pred_der is not None:
            dir_der = 1 if pred_der > last_val else -1 if pred_der < last_val else 0
            
        if dir_abs == 0 and dir_der == 0: return None
        if dir_abs != 0 and dir_der != 0 and dir_abs != dir_der: return None # Conflict
        
        if dir_abs != 0: return pred_abs
        if dir_der != 0: return pred_der
            
    return None

def generate_signal(prices, strategies):
    if not strategies: return 0
    active_signals = []
    
    max_seq_needed = max(s['s_len'] for s in strategies)
    if len(prices) < max_seq_needed + 1: return 0

    for model in strategies:
        b_size = model['bucket_size']
        seq_len = model['s_len']
        
        curr_slice = prices[-(seq_len + 1):]
        buckets = [get_bucket(p, b_size) for p in curr_slice]
        
        if len(buckets) < seq_len: continue
        
        a_seq = tuple(buckets[-seq_len:]) # Sequence leading UP to current
        last_val = a_seq[-1]              # The bucket of the most recent known price
        
        d_seq = tuple(a_seq[k] - a_seq[k-1] for k in range(1, len(a_seq))) if seq_len > 1 else ()
        
        pred_val = get_prediction(
            model['model_type'], 
            model['abs_map'], model['der_map'], 
            a_seq, d_seq, last_val
        )
        
        if pred_val is not None:
            pred_diff = pred_val - last_val
            if pred_diff != 0:
                direction = 1 if pred_diff > 0 else -1
                active_signals.append({"dir": direction})
            
    if not active_signals: return 0
    
    # Conflict check
    directions = {x['dir'] for x in active_signals}
    if len(directions) > 1: return 0 
    
    return active_signals[0]['dir']

# =========================================
# 5. VERIFICATION & HISTORY
# =========================================

def run_strict_verification(models_cache):
    print("\n========================================")
    print("STARTING MODEL VERIFICATION")
    print("========================================")
    
    targets = []
    for asset, tfs in models_cache.items():
        for tf in tfs.keys(): targets.append((asset, tf))
            
    if not targets:
        print("No models found.")
        return False
        
    sample = random.sample(targets, min(3, len(targets)))
    
    data_map = {}
    needed_assets = set(t[0] for t in sample)
    for a in needed_assets:
        data_map[a] = get_binance_recent(a, days=5) # 5 days is enough for recent verification
        
    passed_count = 0
    
    for asset, tf_name in sample:
        model_data = models_cache[asset][tf_name]
        strategies = model_data['strategies']
        
        raw_data = data_map[asset]
        prices = resample_prices(raw_data, TIMEFRAMES[tf_name])
        
        if len(prices) < 100: continue
            
        trades = 0
        correct = 0
        
        # Verify on last 20% of data
        start_idx = int(len(prices) * 0.8)
        max_seq = max(s['s_len'] for s in strategies)
        
        for i in range(start_idx, len(prices) - 1):
            hist = prices[:i+1]
            actual_next = prices[i+1]
            curr_price = prices[i]
            
            sig = generate_signal(hist, strategies)
            
            if sig != 0:
                trades += 1
                diff = actual_next - curr_price
                if (sig == 1 and diff > 0) or (sig == -1 and diff < 0):
                    correct += 1
        
        acc = (correct / trades * 100) if trades > 0 else 0
        print(f"[VERIFY] {asset} {tf_name}: {trades} trades, {acc:.1f}% acc")
        passed_count += 1
            
    return passed_count > 0

def populate_weekly_history(models_cache):
    print("\n--- Generating Signal Log ---")
    global HISTORY_LOG, HISTORY_ACCURACY, TOTAL_PNL
    logs = []
    
    cutoff_time = datetime.now() - timedelta(days=7)
    total_wins = 0
    total_valid = 0
    
    for asset in ASSETS:
        raw_data = PRICE_CACHE.get(asset)
        if not raw_data: continue
            
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            df = get_resampled_df(raw_data, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            max_seq = max(s['s_len'] for s in strategies)
            
            if len(prices) < max_seq + 2: continue

            for i in range(max_seq + 1, len(prices)):
                # i is the outcome index
                # i-1 is the signal index
                
                signal_ts = timestamps[i-1]
                if signal_ts < cutoff_time: continue
                
                hist_prices = prices[:i] # Prices up to i-1 (inclusive)
                
                sig = generate_signal(hist_prices, strategies)
                
                if sig != 0:
                    entry = prices[i-1]
                    exit_p = prices[i]
                    pnl = ((exit_p - entry) / entry) * 100 * sig
                    
                    outcome = "WIN" if pnl > 0 else "LOSS"
                    if abs(pnl) < 0.02: outcome = "NOISE"
                    
                    if outcome != "NOISE":
                        total_valid += 1
                        if outcome == "WIN": total_wins += 1
                        
                    logs.append({
                        "time": signal_ts.strftime("%Y-%m-%d %H:%M"),
                        "asset": asset,
                        "tf": tf_name,
                        "signal": "BUY" if sig == 1 else "SELL",
                        "price_at_signal": entry,
                        "outcome": outcome,
                        "close_price": exit_p,
                        "pnl": pnl
                    })
                    
    logs.sort(key=lambda x: x['time'], reverse=True)
    HISTORY_LOG = logs
    HISTORY_ACCURACY = (total_wins / total_valid * 100) if total_valid > 0 else 0
    TOTAL_PNL = sum(l['pnl'] for l in logs)
    
    # DB Save
    if SessionLocal:
        try:
            session = SessionLocal()
            session.query(HistoryEntry).delete()
            for log in logs:
                entry = HistoryEntry(
                    time_str=log['time'], asset=log['asset'], tf=log['tf'],
                    signal=log['signal'], price_at_signal=log['price_at_signal'],
                    outcome=log['outcome'], close_price=log['close_price'], pnl=log['pnl']
                )
                session.add(entry)
            session.commit()
            session.close()
        except Exception: pass

# =========================================
# 6. LIVE SERVER & SCHEDULER
# =========================================

def prefetch_bulk_data():
    """Runs rarely: Fetches 5 days of history to fill cache."""
    global PRICE_CACHE
    print(f"\n[Prefetch] Bulk downloading history at {datetime.now().strftime('%H:%M:%S')}")
    for asset in ASSETS:
        try:
            PRICE_CACHE[asset] = get_binance_recent(asset, days=5)
        except Exception as e:
            print(f"Error prefetching {asset}: {e}")

def update_live_signals(models_cache):
    """Runs every minute: Fetches tiny data increment."""
    global SIGNAL_MATRIX, LAST_UPDATE, PRICE_CACHE
    
    print(f"[Live Update] {datetime.now().strftime('%H:%M:%S')}")
    temp_matrix = {}
    
    for asset in ASSETS:
        temp_matrix[asset] = {}
        
        # 1. Access Cache
        history = PRICE_CACHE.get(asset, [])
        if not history:
            # First run for this asset? Bulk fetch now.
            history = get_binance_recent(asset, days=5)
            
        # 2. Fetch TINY update (last 3 minutes only)
        # This is the lightweight call
        now_ts = int(time.time() * 1000)
        tiny_data = fetch_binance_segment(asset, now_ts - (3 * 60 * 1000), now_ts)
        
        # 3. Merge & Deduplicate
        data_map = {x[0]: x[1] for x in history}
        for x in tiny_data: 
            data_map[x[0]] = x[1]
            
        full_data = sorted([(k,v) for k,v in data_map.items()])
        
        # Keep cache size manageable (trim to last 7 days approx 10k candles)
        if len(full_data) > 11000:
            full_data = full_data[-10080:]
            
        PRICE_CACHE[asset] = full_data
        
        # 4. Generate Signal
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            prices = resample_prices(full_data, tf_pandas)
            
            # Remove incomplete candle
            if prices: prices = prices[:-1]
                
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save Matrix to DB
    if SessionLocal:
        try:
            session = SessionLocal()
            session.query(MatrixEntry).delete()
            for asset, tfs in temp_matrix.items():
                for tf, val in tfs.items():
                    entry = MatrixEntry(asset=asset, tf=tf, signal_val=val)
                    session.add(entry)
            session.commit()
            session.close()
        except: pass
        
    populate_weekly_history(models_cache)

def run_scheduler(models_cache):
    print("[Scheduler] Initializing...")
    
    # 1. INITIAL HEAVY LOAD
    prefetch_bulk_data()
    update_live_signals(models_cache)
    
    # 2. SCHEDULE LIGHTWEIGHT UPDATE
    # Runs every minute at 2 seconds past the minute
    schedule.every(1).minutes.at(":02").do(update_live_signals, models_cache)
    
    # 3. SCHEDULE SAFETY REFRESH (Rarely)
    # Every 6 hours, refresh full history to ensure no gaps
    schedule.every(6).hours.do(prefetch_bulk_data)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# =========================================
# 7. FLASK APP
# =========================================

app = Flask(__name__)

@app.route('/')
def home():
    acc_color = "#0f0" if HISTORY_ACCURACY > 50 else "#f00"
    pnl_color = "#0f0" if TOTAL_PNL >= 0 else "#f00"
    
    html = f"""
    <html>
    <head>
        <title>Strategy Matrix V2</title>
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
            .pos-pnl {{ color: #0f0; }}
            .neg-pnl {{ color: #f00; }}
            h2 {{ border-bottom: 1px solid #444; padding-bottom: 5px; margin-top: 40px; }}
            .stats-container {{ display: flex; gap: 40px; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h1>Live Signal Matrix (V2)</h1>
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
        
        <h2>Performance (Last 7 Days)</h2>
        <div class="stats-container">
            <div>
                <strong>Accuracy:</strong>
                <span style="color:{acc_color}; font-size: 1.5em; display:block;">{HISTORY_ACCURACY:.2f}%</span>
            </div>
            <div>
                <strong>PnL:</strong>
                <span style="color:{pnl_color}; font-size: 1.5em; display:block;">{TOTAL_PNL:+.2f}%</span>
            </div>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Asset</th>
                    <th>TF</th>
                    <th>Signal</th>
                    <th>Price</th>
                    <th>PnL</th>
                    <th>Outcome</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for log in HISTORY_LOG[:100]:
        cls = "buy" if log['signal'] == "BUY" else "sell"
        res_cls = log['outcome']
        pnl_cls = "pos-pnl" if log['pnl'] >= 0 else "neg-pnl"
        
        html += f"""
        <tr>
            <td>{log['time']}</td>
            <td>{log['asset']}</td>
            <td>{log['tf']}</td>
            <td class='{cls}'>{log['signal']}</td>
            <td>{log['price_at_signal']:.4f}</td>
            <td class='{pnl_cls}'>{log['pnl']:.2f}%</td>
            <td class='{res_cls}'>{log['outcome']}</td>
        </tr>
        """
        
    html += "</tbody></table></body></html>"
    return render_template_string(html)

if __name__ == "__main__":
    models = fetch_models_from_github()
    if run_strict_verification(models):
        print("\n>>> STARTING LIVE SERVER (V2) <<<")
        
        # Start Scheduler in Background
        t = threading.Thread(target=run_scheduler, args=(models,))
        t.daemon = True
        t.start()
        
        # Start Flask
        app.run(host='0.0.0.0', port=8080)
    else:
        print("\n>>> VERIFICATION FAILED. <<<")
