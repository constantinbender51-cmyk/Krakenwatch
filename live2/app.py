import os
import sys
import json
import time
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

ASSETS = {
    "BTCUSDT": {"binance": "BTCUSDT", "kraken": "XBTUSD"},
    "ETHUSDT": {"binance": "ETHUSDT", "kraken": "ETHUSD"},
    "SOLUSDT": {"binance": "SOLUSDT", "kraken": "SOLUSD"},
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
    
    # Reset Signals Table (Active Snapshot)
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

    # History Table (Metrics)
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
# 3. DATA & STRATEGY
# =========================================

def fetch_binance_history_7d(symbol):
    """Fetch 7 days of 1m data"""
    base_url = "https://api.binance.com/api/v3/klines"
    end_time = int(time.time() * 1000)
    start_time = end_time - (7 * 24 * 60 * 60 * 1000)
    
    all_candles = []
    current_start = start_time
    
    print(f"[{symbol}] Downloading History...", end=" ")
    while current_start < end_time:
        try:
            r = requests.get(base_url, params={"symbol": symbol, "interval": "1m", "startTime": current_start, "limit": 1000})
            data = r.json()
            if not data or not isinstance(data, list): break
            
            batch = [{"timestamp": int(c[0]), "price": float(c[4])} for c in data]
            all_candles.extend(batch)
            current_start = batch[-1]["timestamp"] + 1
            time.sleep(0.05)
        except: break
            
    print(f"Done ({len(all_candles)} candles).")
    return all_candles

def fetch_kraken_latest(pair):
    try:
        r = requests.get(f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1", timeout=5)
        res = r.json().get('result', {})
        for k, v in res.items():
            if k != "last" and isinstance(v, list):
                return [{"timestamp": int(c[0]) * 1000, "price": float(c[4])} for c in v[-2:]]
    except: pass
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
            self.data[asset] = combined.sort_index().iloc[-15000:]

    def get_prices(self, asset, tf_alias):
        if asset not in self.data: return []
        df = self.data[asset]
        if tf_alias == "1min": return df['price'] 
        return df['price'].resample(tf_alias).last().dropna()

class StrategyLoader:
    def __init__(self):
        self.models = {}

    def load(self):
        if not GITHUB_PAT: return
        try:
            r = requests.get(GITHUB_API_URL, headers={"Authorization": f"Bearer {GITHUB_PAT}"})
            for f in r.json():
                if f['name'].endswith(".json") and "_" in f['name']:
                    data = requests.get(f['download_url']).json()
                    key = f['name'].replace(".json", "")
                    self.models[key] = data.get('strategies', [])
                    print(f"Loaded Model: {key}")
        except Exception as e:
            print(f"Model Load Failed: {e}")

    def get_bucket(self, price, size):
        return int(price // size) if size > 0 else 0

    def predict(self, asset, tf, price_series):
        """
        UPDATED: Now handles 'Compact' model files where maps 
        contain direct values instead of frequency counters.
        """
        key = f"{asset}_{tf}"
        strategies = self.models.get(key, [])
        if not strategies: return 0
        
        if hasattr(price_series, 'tolist'): prices = price_series.tolist()
        else: prices = price_series
            
        if len(prices) < 50: return 0
        
        votes = 0
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
            
            # --- UPDATED LOGIC FOR COMPACT MAPS ---
            # Direct dictionary lookup. No more max() needed.
            
            if cfg['model'] == "Absolute" and a_seq in p['abs_map']:
                pred_val = p['abs_map'][a_seq] # Direct access
            
            elif cfg['model'] == "Derivative" and d_seq in p['der_map']:
                change = p['der_map'][d_seq]   # Direct access
                pred_val = bkts[-1] + change
            
            # Combined logic: only if individual parts exist
            elif cfg['model'] == "Combined":
                val_abs = p['abs_map'].get(a_seq)
                val_der = p['der_map'].get(d_seq)
                
                if val_abs is not None and val_der is not None:
                     # Calculate derivative prediction value
                    pred_der_val = bkts[-1] + val_der
                    
                    # Direction check
                    dir_a = 1 if val_abs > bkts[-1] else -1 if val_abs < bkts[-1] else 0
                    dir_d = 1 if pred_der_val > bkts[-1] else -1 if pred_der_val < bkts[-1] else 0
                    
                    if dir_a == dir_d and dir_a != 0:
                        pred_val = val_abs # (Value doesn't matter much if direction agrees)

            if pred_val is not None:
                if pred_val > bkts[-1]: votes += 1
                elif pred_val < bkts[-1]: votes -= 1
        
        return 1 if votes > 0 else -1 if votes < 0 else 0

# =========================================
# 4. BACKFILL LOGIC
# =========================================

def run_backfill(conn, market, engine):
    print("\n--- Starting Backfill (Generating History) ---")
    total_trades = 0
    
    for asset in ASSETS.keys():
        for tf_name, tf_alias in TIMEFRAMES.items():
            
            if f"{asset}_{tf_name}" not in engine.models:
                continue
                
            full_series = market.get_prices(asset, tf_alias)
            if len(full_series) < 100: continue
            
            timestamps = full_series.index
            prices = full_series.values
            
            active_signal = 0
            entry_price = 0.0
            
            # Simulating history
            for i in range(50, len(prices) - 1):
                # Close previous
                if active_signal != 0:
                    record_trade_result(conn, asset, tf_name, active_signal, entry_price, prices[i], timestamps[i])
                    total_trades += 1
                    active_signal = 0 
                
                # Predict next
                current_slice = prices[i-20:i+1]
                sig = engine.predict(asset, tf_name, current_slice)
                
                # Open new
                if sig != 0:
                    active_signal = sig
                    entry_price = prices[i]
                    
            print(f"Backfilled {asset} {tf_name}")
            
    print(f"Backfill Complete. {total_trades} historical trades generated.\n")

# =========================================
# 5. MAIN LOOP
# =========================================

def main():
    init_db()
    conn = get_db_connection()
    
    market = MarketBuffer()
    engine = StrategyLoader()
    engine.load()
    
    # 1. FETCH HISTORY
    for model_sym, pairs in ASSETS.items():
        hist = fetch_binance_history_7d(pairs['binance'])
        market.ingest(model_sym, hist)
        
    # 2. BACKFILL
    run_backfill(conn, market, engine)
    compute_7d_metrics(conn)
    
    # 3. LIVE LOOP
    print("Entering Live Mode...")
    while True:
        now = datetime.now()
        sleep_s = 60 - now.second + 2
        print(f"Waiting {sleep_s}s...")
        time.sleep(sleep_s)
        
        now_dt = datetime.now()
        
        for model_sym, pairs in ASSETS.items():
            kraken_pair = pairs['kraken']
            k_data = fetch_kraken_latest(kraken_pair)
            if not k_data or len(k_data) < 2: continue
            
            closed_candle = k_data[-2]
            curr_price = closed_candle['price']
            market.ingest(model_sym, [closed_candle])
            
            for tf_name, tf_pd in TIMEFRAMES.items():
                
                # Check Boundary
                m = now_dt.minute
                is_boundary = False
                if tf_name == "1m": is_boundary = True
                elif tf_name == "5m" and m % 5 == 0: is_boundary = True
                elif tf_name == "15m" and m % 15 == 0: is_boundary = True
                elif tf_name == "30m" and m % 30 == 0: is_boundary = True
                elif tf_name == "1h" and m == 0: is_boundary = True
                
                if is_boundary:
                    # Close previous signal
                    prev_key = f"{model_sym}_{tf_name}"
                    if prev_key in market.last_signal:
                        old = market.last_signal[prev_key]
                        record_trade_result(conn, model_sym, tf_name, old['sig'], old['entry'], curr_price, now_dt)
                    
                    # Predict
                    prices = market.get_prices(model_sym, tf_pd)
                    sig = engine.predict(model_sym, tf_name, prices)
                    
                    # Upsert Active Signal
                    if sig != 0:
                        delta = int(tf_pd.replace("min","").replace("H","60").replace("1min","1"))
                        end_t = now_dt + timedelta(minutes=delta)
                        overwrite_signal(conn, model_sym, tf_name, sig, now_dt, end_t)
                        market.last_signal[prev_key] = {"sig": sig, "entry": curr_price}
                        print(f">> {model_sym} {tf_name}: SIGNAL {sig}")
        
        compute_7d_metrics(conn)

if __name__ == "__main__":
    main()
