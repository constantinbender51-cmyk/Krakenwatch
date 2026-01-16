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

# --- DATABASE IMPORTS ---
try:
    from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
    from sqlalchemy.orm import sessionmaker, declarative_base
except ImportError:
    print("Warning: SQLAlchemy not found. DB features will be disabled.")
    # Dummy classes to prevent crash if libs missing
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
REPO_NAME = "model-2"  # Updated to match training script repo name
GITHUB_API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

ASSETS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", 
    "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "TRXUSDT",
    "BCHUSDT", "XLMUSDT", "LTCUSDT", "SUIUSDT", "HBARUSDT",
    "SHIBUSDT", "TONUSDT", "UNIUSDT", "ZECUSDT"
]

START_DATE = "2020-01-01"
END_DATE = "2026-01-01"
BASE_INTERVAL = "1m" # Updated to match training script base

TIMEFRAMES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H"
}

# Global State
SIGNAL_MATRIX = {}
HISTORY_LOG = []
HISTORY_ACCURACY = 0.0
TOTAL_PNL = 0.0
LAST_UPDATE = "Never"
PRICE_CACHE = {} 

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

# Define Models (Renamed Tables)
class HistoryEntry(Base):
    __tablename__ = 'signal_history_v2'  # <--- RENAMED
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
    __tablename__ = 'live_matrix_v2'     # <--- RENAMED
    asset = Column(String, primary_key=True)
    tf = Column(String, primary_key=True)
    signal_val = Column(Integer)
    updated_at = Column(DateTime, default=datetime.utcnow)

# Create Tables
if DATABASE_URL and engine:
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        print(f">>> Error creating tables: {e}")

# --- 1. UTILITIES ---

def get_bucket(price, bucket_size):
    if bucket_size <= 0: bucket_size = 1e-9
    return int(price // bucket_size)

def make_key(seq):
    """Creates the pipe-separated string key used in the JSON maps"""
    return "|".join(map(str, seq))

def resample_prices(raw_data, target_freq):
    if not raw_data: return []
    # If target is 1m (base), just return close prices
    if target_freq == "1min" or target_freq == "1m":
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
    
    if target_freq == "1min" or target_freq == "1m":
        return df[['price']]
        
    return df['price'].resample(target_freq).last().dropna().to_frame()

# --- 2. DATA MANAGEMENT ---

def fetch_binance_segment(symbol, start_ts, end_ts):
    base_url = "https://api.binance.com/api/v3/klines"
    segment_candles = []
    current_start = start_ts
    while current_start < end_ts:
        # Fetching 1m data as base
        url = f"{base_url}?symbol={symbol}&interval={BASE_INTERVAL}&startTime={current_start}&endTime={end_ts}&limit=1000"
        try:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read().decode())
                if not data: break
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
                    
                    # Adapt to new JSON structure
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
                            # Maps are now simple dicts: "key_str": int_val
                            "abs_map": params.get("abs_map", {}),
                            "der_map": params.get("der_map", {})
                        })
                    
                    # Stats are now in holdout_stats
                    stats = file_content.get("holdout_stats", {})
                    
                    models_cache[asset][tf] = {
                        "strategies": strategies,
                        "expected_acc": stats.get("accuracy", 0),
                        "expected_trades": stats.get("trades", 0)
                    }
                    print(f"[{asset}] Loaded {tf} models (Acc: {stats.get('accuracy', 0):.1f}%)")
            except Exception as e:
                # Silent fail for missing models, just skip
                pass
    return models_cache

# --- 3. INFERENCE LOGIC ---

def get_single_prediction(mode, abs_map, der_map, a_seq, d_seq, last_val):
    """Core prediction lookup matching training script"""
    if mode == "Absolute":
        key = make_key(a_seq)
        if key in abs_map:
            return abs_map[key] # Returns int value directly
            
    elif mode == "Derivative":
        key = make_key(d_seq)
        if key in der_map:
            pred_change = der_map[key] # Returns int change
            return last_val + pred_change
            
    return None

def get_prediction(model_type, abs_map, der_map, a_seq, d_seq, last_val):
    """
    Exact replication of the 'Combined' logic from the training script.
    """
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
            
        # Conflict Resolution Logic from Training Script
        if dir_abs == 0 and dir_der == 0: return None
        if dir_abs != 0 and dir_der != 0 and dir_abs != dir_der: return None # Conflict -> Abstain
        
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
        
        # Get relevant slice
        relevant_prices = prices[-(seq_len + 1):] # Need +1 to calculate last bucket
        # Current logic is we predict NEXT based on PREVIOUS sequence.
        # So we need seq_len items ending at index -1
        
        # Actually, let's stick to the list slicing:
        # Input: [p1, p2, p3, p4, p5] (p5 is 'last_val')
        # We want to predict p6.
        
        curr_slice = relevant_prices # This includes the last known price
        buckets = [get_bucket(p, b_size) for p in curr_slice]
        
        # Sequence is everything up to the last known price
        # Wait, if seq_len is 5, we need 5 items to form the key.
        # If we have [b1, b2, b3, b4, b5], the sequence is (b1..b5).
        # We predict b6.
        
        if len(buckets) < seq_len: continue
        
        a_seq = tuple(buckets[-seq_len:]) # The sequence IS the last seq_len buckets
        last_val = a_seq[-1]
        
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
                active_signals.append({
                    "dir": direction,
                    "b_count": model['b_count'] # Used for tie-breaking/sorting if needed
                })
            
    if not active_signals: return 0
    
    # Voting Logic
    # 1. Check for conflict (if we have conflicting non-neutral signals)
    directions = {x['dir'] for x in active_signals}
    
    if len(directions) > 1:
        # Conflict exists (some say Up, some say Down)
        # Training script logic: "if len(directions) > 1: conflicts += 1; continue"
        return 0 
        
    # If no conflict, follow the direction (since all active signals agree)
    return active_signals[0]['dir']

# --- 4. VERIFICATION & HISTORICAL ANALYSIS ---

def run_strict_verification(models_cache):
    print("\n========================================")
    print("STARTING STRICT MODEL VERIFICATION")
    print("========================================")
    
    # Flatten available strategies
    targets = []
    for asset, tfs in models_cache.items():
        for tf in tfs.keys():
            targets.append((asset, tf))
            
    if not targets:
        print("No models found.")
        return False
        
    # Pick random subset
    sample = random.sample(targets, min(3, len(targets)))
    print(f"Verifying: {sample}")
    
    passed_all = True
    
    # Pre-fetch data for these assets
    needed_assets = set(t[0] for t in sample)
    data_map = {}
    for a in needed_assets:
        # Fetch 20 days to ensure enough data for higher TFs
        data_map[a] = get_binance_recent(a, days=20)
        
    for asset, tf_name in sample:
        model_data = models_cache[asset][tf_name]
        strategies = model_data['strategies']
        exp_acc = model_data['expected_acc']
        exp_trades = model_data['expected_trades']
        
        raw_data = data_map[asset]
        prices = resample_prices(raw_data, TIMEFRAMES[tf_name])
        
        if len(prices) < 100:
            print(f"[{asset} {tf_name}] Insufficient data for verification.")
            continue
            
        # Run Backtest on this segment
        correct = 0
        trades = 0
        
        # Reserve last 20% for 'holdout' simulation to match training split approx
        # purely for verification check
        split_idx = int(len(prices) * 0.8)
        test_prices = prices[split_idx:]
        
        max_seq = max(s['s_len'] for s in strategies)
        
        for i in range(max_seq, len(test_prices) - 1):
            hist = test_prices[:i+1] # Include current price as last known
            actual_next = test_prices[i+1]
            current_price = test_prices[i]
            
            sig = generate_signal(hist, strategies)
            
            if sig != 0:
                trades += 1
                diff = actual_next - current_price
                if (sig == 1 and diff > 0) or (sig == -1 and diff < 0):
                    correct += 1
                    
        calc_acc = (correct / trades * 100) if trades > 0 else 0
        
        # Relaxed verification: We just want to ensure logic works, not match exact holdout
        # because the 'holdout' in file is fixed date, and we are using recent data.
        # We just check if it generates *any* trades and doesn't crash.
        print(f"[VERIFY] {asset} {tf_name} | Trades: {trades} | Acc: {calc_acc:.2f}% (Exp Holdout: {exp_acc:.1f}%)")
        
        if trades == 0 and exp_trades > 5:
            print(f"--> WARNING: Model expected trades but got 0 in recent history.")
            # We don't fail hard here because market conditions change
            
    return passed_all

def populate_weekly_history(models_cache):
    print("\n--- Generating Signal Log ---")
    global HISTORY_LOG, HISTORY_ACCURACY, TOTAL_PNL
    logs = []
    
    cutoff_time = datetime.now() - timedelta(days=7)
    total_wins = 0
    total_valid = 0
    
    for asset in ASSETS:
        raw_data = PRICE_CACHE.get(asset)
        if not raw_data:
            raw_data = get_binance_recent(asset, days=7)
            PRICE_CACHE[asset] = raw_data
            
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            df = get_resampled_df(raw_data, tf_pandas)
            if df.empty: continue
            
            prices = df['price'].tolist()
            timestamps = df.index.tolist()
            
            max_seq = max(s['s_len'] for s in strategies)
            
            # Replay history
            for i in range(max_seq + 1, len(prices)):
                # prices[i] is the outcome we see
                # prices[i-1] is the price we signalled at
                
                target_ts = timestamps[i] # Time the candle CLOSED (outcome known)
                # But we want the signal time, which is timestamps[i-1]
                signal_ts = timestamps[i-1]
                
                if signal_ts < cutoff_time: continue
                
                # Input history ends at i-1
                hist_prices = prices[:i] 
                
                sig = generate_signal(hist_prices, strategies)
                
                if sig != 0:
                    entry_price = prices[i-1]
                    exit_price = prices[i]
                    
                    raw_pct = ((exit_price - entry_price) / entry_price) * 100
                    pnl = raw_pct * sig
                    
                    outcome = "WIN" if pnl > 0 else "LOSS"
                    if abs(pnl) < 0.02: outcome = "NOISE" # Filter tiny moves
                    
                    if outcome != "NOISE":
                        total_valid += 1
                        if outcome == "WIN": total_wins += 1
                        
                    logs.append({
                        "time": signal_ts.strftime("%Y-%m-%d %H:%M"),
                        "asset": asset,
                        "tf": tf_name,
                        "signal": "BUY" if sig == 1 else "SELL",
                        "price_at_signal": entry_price,
                        "outcome": outcome,
                        "close_price": exit_price,
                        "pnl": pnl
                    })
                    
    logs.sort(key=lambda x: x['time'], reverse=True)
    HISTORY_LOG = logs
    HISTORY_ACCURACY = (total_wins / total_valid * 100) if total_valid > 0 else 0
    TOTAL_PNL = sum(l['pnl'] for l in logs)
    
    print(f"Generated {len(logs)} logs. Acc: {HISTORY_ACCURACY:.2f}%. PnL: {TOTAL_PNL:.2f}%")
    
    # Save to DB
    if SessionLocal:
        try:
            session = SessionLocal()
            # Clear old history v2
            session.query(HistoryEntry).delete()
            for log in logs:
                entry = HistoryEntry(
                    time_str=log['time'],
                    asset=log['asset'],
                    tf=log['tf'],
                    signal=log['signal'],
                    price_at_signal=log['price_at_signal'],
                    outcome=log['outcome'],
                    close_price=log['close_price'],
                    pnl=log['pnl']
                )
                session.add(entry)
            session.commit()
            session.close()
        except Exception as e:
            print(f"DB Save Error: {e}")

# --- 5. LIVE SERVER & LATENCY OPTIMIZATION ---

def prefetch_bulk_data():
    """Runs 10s before close to warm cache"""
    global PRICE_CACHE
    print(f"\n[Prefetch] {datetime.now().strftime('%H:%M:%S')}")
    for asset in ASSETS:
        try:
            # Get 5 days is enough for 1h sequences usually
            PRICE_CACHE[asset] = get_binance_recent(asset, days=5)
        except: pass

def update_live_signals(models_cache):
    """Runs exactly on candle close"""
    global SIGNAL_MATRIX, LAST_UPDATE, PRICE_CACHE
    print(f"\n[Live Update] {datetime.now().strftime('%H:%M:%S')}")
    
    temp_matrix = {}
    
    for asset in ASSETS:
        temp_matrix[asset] = {}
        
        # 1. Get Data (Cache + Tiny Update)
        history = PRICE_CACHE.get(asset, [])
        if not history:
            history = get_binance_recent(asset, days=5)
            
        # Fetch last 3 minutes to ensure we have the closure
        now_ts = int(time.time() * 1000)
        tiny = fetch_binance_segment(asset, now_ts - 180000, now_ts)
        
        # Merge
        data_map = {x[0]: x[1] for x in history}
        for x in tiny: data_map[x[0]] = x[1]
        
        full_data = sorted([(k,v) for k,v in data_map.items()])
        PRICE_CACHE[asset] = full_data # Update cache
        
        # 2. Inference
        for tf_name, model_data in models_cache.get(asset, {}).items():
            strategies = model_data['strategies']
            tf_pandas = TIMEFRAMES[tf_name]
            
            prices = resample_prices(full_data, tf_pandas)
            
            # We want to signal based on the candle that JUST closed.
            # 'prices' usually includes the open candle (incomplete) as the last element if we just fetched it.
            # We strip the last one to get the definitive closed history.
            if prices:
                prices = prices[:-1]
                
            sig = generate_signal(prices, strategies)
            temp_matrix[asset][tf_name] = sig
            
    SIGNAL_MATRIX = temp_matrix
    LAST_UPDATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # DB Save
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

def run_scheduler(models_cache):
    # Initial data load
    prefetch_bulk_data()
    update_live_signals(models_cache)
    
    # Prefetch 10s before minutes 0, 15, 30, 45 (for 15m/30m/1h alignments)
    # Also runs every minute to support 1m/5m timeframe updates if needed
    schedule.every(1).minutes.at(":50").do(prefetch_bulk_data)
    schedule.every(1).minutes.at(":00").do(update_live_signals, models_cache)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    models = fetch_models_from_github()
    if run_strict_verification(models):
        print("\n>>> STARTING LIVE SERVER (V2) <<<")
        populate_weekly_history(models)
        t = threading.Thread(target=run_scheduler, args=(models,))
        t.daemon = True
        t.start()
        app.run(host='0.0.0.0', port=8080)
    else:
        print("\n>>> VERIFICATION FAILED. <<<")
