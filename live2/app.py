import os
import sys
import json
import time
import random
import requests
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from urllib.error import HTTPError

# =========================================
# 1. CONFIGURATION
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = os.getenv("REPO_OWNER", "constantinbender51-cmyk")
REPO_NAME = os.getenv("REPO_NAME", "model-2")
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"
DATABASE_URL = os.getenv("DATABASE_URL")

# NOTE: Using Binance tickers for both live and model mapping to ensure consistency
ASSETS = {
    "BTCUSDT": "BTCUSDT",
    "ETHUSDT": "ETHUSDT",
    "SOLUSDT": "SOLUSDT",
}

TIMEFRAMES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H"
}

# =========================================
# 2. DATABASE
# =========================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS signals;")
    cur.execute("""
        CREATE TABLE signals (
            asset VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            signal INT NOT NULL,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (asset, timeframe)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id SERIAL PRIMARY KEY,
            asset VARCHAR(20),
            timeframe VARCHAR(10),
            signal INT,
            entry_price DOUBLE PRECISION,
            exit_price DOUBLE PRECISION,
            pnl_pct DOUBLE PRECISION,
            closed_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")

def overwrite_signal(conn, asset, tf, signal, start_dt, end_dt):
    try:
        cur = conn.cursor()
        query = """
            INSERT INTO signals (asset, timeframe, signal, start_time, end_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (asset, timeframe) 
            DO UPDATE SET 
                signal = EXCLUDED.signal,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time;
        """
        cur.execute(query, (asset, tf, int(signal), start_dt, end_dt))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Error] Signal Overwrite: {e}")

def record_trade_result(conn, asset, tf, signal, entry_price, exit_price, timestamp_dt):
    if signal == 0 or entry_price == 0: return
    pnl = ((exit_price - entry_price) / entry_price) * signal
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO history (asset, timeframe, signal, entry_price, exit_price, pnl_pct, closed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (asset, tf, int(signal), float(entry_price), float(exit_price), float(pnl), timestamp_dt))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Error] History Save: {e}")

def compute_7d_metrics(conn):
    try:
        cur = conn.cursor()
        since = datetime.now() - timedelta(days=7)
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                SUM(pnl_pct) as total_pnl
            FROM history
            WHERE closed_at >= %s
        """, (since,))
        row = cur.fetchone()
        cur.close()
        if row and row[0] > 0:
            acc = (row[1] / row[0]) * 100
            print(f"\n[METRICS] Trades: {row[0]} | Acc: {acc:.2f}% | PnL: {row[2]*100:.2f}%")
        else:
            print("\n[METRICS] No closed trades yet.")
    except Exception:
        pass

# =========================================
# 3. DATA UTILITIES
# =========================================

def fetch_binance_history_custom(symbol, interval="1m", days=7):
    base_url = "https://api.binance.com/api/v3/klines"
    end_time = int(time.time() * 1000)
    start_time = end_time - (days * 24 * 60 * 60 * 1000)
    
    all_candles = []
    current_start = start_time
    
    while current_start < end_time:
        try:
            r = requests.get(base_url, params={
                "symbol": symbol, 
                "interval": interval, 
                "startTime": current_start, 
                "limit": 1000
            })
            data = r.json()
            if not data or not isinstance(data, list): break
            
            batch = [{"timestamp": int(c[0]), "price": float(c[4])} for c in data]
            all_candles.extend(batch)
            
            last_ts = batch[-1]["timestamp"]
            if last_ts == current_start: break 
            current_start = last_ts + 1
            
            time.sleep(0.05)
        except: break
            
    return all_candles

def fetch_binance_latest(symbol):
    """
    Fetches the latest completed candle from Binance.
    Matches the data source used in training (Critical for 'Absolute' models).
    """
    try:
        # Limit 2 gets the last closed candle + the current open candle
        r = requests.get("https://api.binance.com/api/v3/klines", params={
            "symbol": symbol, "interval": "1m", "limit": 2
        }, timeout=5)
        data = r.json()
        if data and isinstance(data, list) and len(data) >= 2:
            # Return the second to last (latest CLOSED) candle
            c = data[-2]
            return [{"timestamp": int(c[0]), "price": float(c[4])}]
    except Exception as e:
        print(f"Binance Live Fetch Error: {e}")
    return None

class MarketBuffer:
    def __init__(self):
        self.data = {} 
        self.last_signal = {} 

    def ingest(self, asset, candles_list):
        if not candles_list: return
        df = pd.DataFrame(candles_list)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        if asset not in self.data:
            self.data[asset] = df
        else:
            combined = pd.concat([self.data[asset], df])
            combined = combined[~combined.index.duplicated(keep='last')]
            # Keep last ~14 days of 1m data to safely resample 1h
            self.data[asset] = combined.sort_index().iloc[-20160:]

    def get_prices(self, asset, tf_alias):
        if asset not in self.data: return []
        df = self.data[asset]
        
        if tf_alias == "1min": 
            return df['price']
            
        # Resample logic:
        # We must drop the last bin if it's incomplete (current time) to avoid false signals
        resampled = df['price'].resample(tf_alias).last().dropna()
        
        # Simple check: If the last timestamp is essentially "now" and the timeframe is large,
        # it might be an open candle. (Pandas usually labels by left edge, but safety first).
        # For simplicity in this script, we rely on the caller (main loop) only triggering
        # on boundaries, but resampling correctly is key.
        return resampled

class StrategyLoader:
    def __init__(self):
        self.models = {}
        self.specs = {}

    def load(self):
        if not GITHUB_PAT: return
        try:
            r = requests.get(GITHUB_API_URL, headers={"Authorization": f"Bearer {GITHUB_PAT}"})
            for f in r.json():
                if f['name'].endswith(".json") and "_" in f['name']:
                    data = requests.get(f['download_url']).json()
                    key = f['name'].replace(".json", "")
                    self.models[key] = data.get('strategies', [])
                    self.specs[key] = data.get('holdout_stats', {})
                    print(f"Loaded Model: {key}")
        except Exception as e:
            print(f"Model Load Failed: {e}")

    def get_bucket(self, price, size):
        return int(price // size) if size > 0 else 0

    def predict(self, asset, tf, price_series):
        """
        PREDICTION ENGINE
        """
        key = f"{asset}_{tf}"
        strategies = self.models.get(key, [])
        if not strategies: return 0
        
        if hasattr(price_series, 'tolist'): prices = price_series.tolist()
        else: prices = price_series
            
        # FIX: Lowered from 50 to 15.
        # Max s_len is typically 12. We need at least s_len + 1 data points.
        if len(prices) < 15: return 0
        
        active_directions = set()
        
        for strat in strategies:
            cfg = strat['config']
            p = strat['params']
            s_len = cfg['s_len']
            
            if len(prices) < s_len + 1: continue
            
            relevant = prices[-(s_len+1):]
            bkts = [self.get_bucket(v, p['bucket_size']) for v in relevant]
            
            a_seq = "|".join(map(str, bkts[:-1]))
            d_seq = ""
            if s_len > 1:
                d_seq = "|".join(map(str, [bkts[i] - bkts[i-1] for i in range(1, len(bkts))]))

            pred_val = None
            
            # --- Logic from generator ---
            if cfg['model'] == "Absolute" and a_seq in p['abs_map']:
                pred_val = p['abs_map'][a_seq]
            
            elif cfg['model'] == "Derivative" and d_seq in p['der_map']:
                change = p['der_map'][d_seq]
                pred_val = bkts[-1] + change
            
            elif cfg['model'] == "Combined":
                val_abs = p['abs_map'].get(a_seq)
                val_der = p['der_map'].get(d_seq)
                
                pred_abs = val_abs
                pred_der = (bkts[-1] + val_der) if val_der is not None else None
                
                dir_abs = 0
                if pred_abs is not None:
                    dir_abs = 1 if pred_abs > bkts[-1] else -1 if pred_abs < bkts[-1] else 0
                
                dir_der = 0
                if pred_der is not None:
                    dir_der = 1 if pred_der > bkts[-1] else -1 if pred_der < bkts[-1] else 0
                
                if dir_abs != 0 and dir_der != 0 and dir_abs != dir_der:
                    pred_val = None # Conflict
                elif dir_abs != 0:
                    pred_val = pred_abs
                elif dir_der != 0:
                    pred_val = pred_der

            if pred_val is not None:
                diff = pred_val - bkts[-1]
                if diff != 0:
                    direction = 1 if diff > 0 else -1
                    active_directions.add(direction)
        
        # Ensemble Consensus
        if not active_directions: return 0 
        if len(active_directions) > 1: return 0 # Conflict
        return list(active_directions)[0]

# =========================================
# 4. FAST 1H VALIDATION
# =========================================

def validate_random_1h_model(engine):
    if not engine.models:
        print("Validation Skipped: No models loaded.")
        return

    candidates = [k for k in engine.models.keys() if k.endswith("_1h")]
    
    if not candidates:
        print("Validation Skipped: No '1h' models found.")
        return

    selected_key = random.choice(candidates)
    asset_pair = selected_key.split("_")[0]
    
    print(f"\n[FAST VALIDATION] Selected 1h Model: {selected_key}")
    print(f"[FAST VALIDATION] Fetching 216 days (7.2mo) of NATIVE 1h data...")
    
    # Ensure we use the Binance ticker
    ticker = ASSETS.get(asset_pair, asset_pair)

    raw_1h_data = fetch_binance_history_custom(
        ticker, 
        interval="1h", 
        days=216
    )
    
    if not raw_1h_data:
        print("CRITICAL: Could not fetch history data. Exiting.")
        sys.exit(0)

    prices = [x['price'] for x in raw_1h_data]
    
    trades = 0
    correct = 0
    active_signal = 0
    last_val = 0
    
    print(f"[FAST VALIDATION] Running backtest on {len(prices)} candles...")
    
    # FIX: Loop range adapted for lower slice requirement
    # We need at least 15 candles. 
    start_idx = 20 
    
    for i in range(start_idx, len(prices) - 1):
        if active_signal != 0:
            change = prices[i] - last_val
            if (active_signal == 1 and change > 0) or (active_signal == -1 and change < 0):
                correct += 1
            if change != 0: 
                trades += 1
            active_signal = 0

        # FIX: Ensure slice is sufficient (20 > 15)
        current_slice = prices[i-20:i+1]
        sig = engine.predict(asset_pair, "1h", current_slice)
        
        if sig != 0:
            active_signal = sig
            last_val = prices[i]
            
    val_acc = (correct / trades * 100) if trades > 0 else 0
    
    spec = engine.specs.get(selected_key, {})
    spec_acc = spec.get('accuracy', 0)
    spec_trades = spec.get('trades', 0)
    
    print(f"\n--- VALIDATION RESULTS ({selected_key}) ---")
    print(f"{'Metric':<15} | {'Spec':<20} | {'Validation':<20}")
    print("-" * 60)
    print(f"{'Accuracy':<15} | {spec_acc:.2f}%{'':<14} | {val_acc:.2f}%")
    print(f"{'Trades':<15} | {spec_trades:<20} | {trades:<20}")
    print("-" * 60)
    
    # Graceful Exit Logic
    if trades == 0 and spec_trades > 0:
         print(f"CRITICAL: Validation generated 0 trades vs Spec {spec_trades} trades.")
         print("Logic mismatch or price drift detected. Exiting gracefully.")
         sys.exit(0)

    deviation = abs(val_acc - spec_acc)
    if deviation > 5.0:
        print(f"CRITICAL: Validation deviation {deviation:.2f}% exceeds 5% limit!")
        print("Model out of sync. Exiting gracefully.")
        sys.exit(0)
        
    print(">> PASS: Model behavior confirmed within tolerance.\n")


# =========================================
# 5. MAIN LOOP
# =========================================

def main():
    init_db()
    conn = get_db_connection()
    
    market = MarketBuffer()
    engine = StrategyLoader()
    engine.load()
    
    # --- STEP 1: FAST 1H VALIDATION ---
    validate_random_1h_model(engine)
    
    # --- STEP 2: LOAD LIVE CONTEXT ---
    print("Fetching 7-day 1m history for live operations...")
    for model_sym, ticker in ASSETS.items():
        hist = fetch_binance_history_custom(ticker, interval="1m", days=7)
        market.ingest(model_sym, hist)
        
    # --- STEP 3: BACKFILL METRICS ---
    print("\n--- Starting Backfill ---")
    total_trades = 0
    for asset in ASSETS.keys():
        for tf_name, tf_alias in TIMEFRAMES.items():
            if f"{asset}_{tf_name}" not in engine.models: continue
            
            full_series = market.get_prices(asset, tf_alias)
            if len(full_series) < 100: continue
            
            timestamps = full_series.index
            prices = full_series.values
            active_signal = 0
            entry_price = 0.0
            
            for i in range(50, len(prices) - 1):
                if active_signal != 0:
                    record_trade_result(conn, asset, tf_name, active_signal, entry_price, prices[i], timestamps[i])
                    total_trades += 1
                    active_signal = 0 
                
                current_slice = prices[i-20:i+1]
                sig = engine.predict(asset, tf_name, current_slice)
                
                if sig != 0:
                    active_signal = sig
                    entry_price = prices[i]
    
    print(f"Backfill Complete. {total_trades} trades generated.\n")
    compute_7d_metrics(conn)
    
    # --- STEP 4: LIVE LOOP ---
    print("Entering Live Mode...")
    while True:
        now = datetime.now()
        # Sleep until 2 seconds into the next minute
        sleep_s = 60 - now.second + 2
        print(f"Waiting {sleep_s}s...")
        time.sleep(sleep_s)
        
        now_dt = datetime.now()
        
        for model_sym, ticker in ASSETS.items():
            # FIX: Use Binance Live data instead of Kraken
            # This ensures price buckets match the training data
            bin_data = fetch_binance_latest(ticker)
            if not bin_data: continue
            
            closed_candle = bin_data[0] # List containing one dict
            curr_price = closed_candle['price']
            market.ingest(model_sym, bin_data)
            
            for tf_name, tf_pd in TIMEFRAMES.items():
                m = now_dt.minute
                is_boundary = False
                if tf_name == "1m": is_boundary = True
                elif tf_name == "5m" and m % 5 == 0: is_boundary = True
                elif tf_name == "15m" and m % 15 == 0: is_boundary = True
                elif tf_name == "30m" and m % 30 == 0: is_boundary = True
                elif tf_name == "1h" and m == 0: is_boundary = True
                
                if is_boundary:
                    prev_key = f"{model_sym}_{tf_name}"
                    
                    # 1. Check if we need to close an old trade
                    if prev_key in market.last_signal:
                        old = market.last_signal[prev_key]
                        record_trade_result(conn, model_sym, tf_name, old['sig'], old['entry'], curr_price, now_dt)
                        del market.last_signal[prev_key] # Reset
                    
                    # 2. Get prediction for next period
                    prices = market.get_prices(model_sym, tf_pd)
                    sig = engine.predict(model_sym, tf_name, prices)
                    
                    if sig != 0:
                        delta = int(tf_pd.replace("min","").replace("H","60").replace("1min","1"))
                        end_t = now_dt + timedelta(minutes=delta)
                        overwrite_signal(conn, model_sym, tf_name, sig, now_dt, end_t)
                        market.last_signal[prev_key] = {"sig": sig, "entry": curr_price}
                        print(f">> {model_sym} {tf_name}: SIGNAL {sig}")
        
        compute_7d_metrics(conn)

if __name__ == "__main__":
    main()
