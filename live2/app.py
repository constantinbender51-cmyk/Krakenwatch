import os
import sys
import json
import time
import requests
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from urllib.error import HTTPError

# =========================================
# 1. CONFIGURATION & SETUP
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# GitHub Config
GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = os.getenv("REPO_OWNER", "constantinbender51-cmyk")
REPO_NAME = os.getenv("REPO_NAME", "model-2")
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# Database Config
DATABASE_URL = os.getenv("DATABASE_URL")

# Asset Mapping (Model Symbol -> Kraken Pair)
# Kraken public API often uses XBTUSD, ETHUSD, etc.
ASSET_MAP = {
    "BTCUSDT": "XBTUSD",
    "ETHUSDT": "ETHUSD",
    "SOLUSDT": "SOLUSD"
}
REVERSE_ASSET_MAP = {v: k for k, v in ASSET_MAP.items()}

# Timeframes to process
TIMEFRAMES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1H"
}

# =========================================
# 2. DATABASE UTILITIES
# =========================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    """Create necessary tables if they don't exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Table 1: Live Signals (Low Latency)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            asset VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            signal INT NOT NULL, -- 1, -1, 0
            price_at_signal DOUBLE PRECISION,
            expiration_time TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_signals_lookup ON signals(asset, timeframe, timestamp);
    """)

    # Table 2: Historical Performance (Metrics)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS performance (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            asset VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            accuracy_7d DOUBLE PRECISION,
            pnl_7d DOUBLE PRECISION,
            total_trades_7d INT,
            last_pnl DOUBLE PRECISION
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialized.")

def save_signal(conn, asset, tf, signal, price, current_ts_dt):
    """Low latency insert of the new prediction."""
    try:
        cur = conn.cursor()
        # Calculate expiration based on timeframe
        delta_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
        expiration = current_ts_dt + timedelta(minutes=delta_map.get(tf, 1))
        
        cur.execute("""
            INSERT INTO signals (timestamp, asset, timeframe, signal, price_at_signal, expiration_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (current_ts_dt, asset, tf, int(signal), float(price), expiration))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Error] Failed to save signal: {e}")

def update_metrics(conn, asset, tf, current_price, current_ts_dt):
    """
    Computes PnL for the PREVIOUS finished candle and updates 7D rolling stats.
    This is run AFTER the signal is sent to minimize latency.
    """
    try:
        cur = conn.cursor()
        
        # 1. Find the signal that just expired (the one from 1 candle ago)
        # For 1m, we look for timestamp = current_ts - 1m
        delta_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60}
        prev_ts = current_ts_dt - timedelta(minutes=delta_map.get(tf, 1))
        
        cur.execute("""
            SELECT signal, price_at_signal FROM signals 
            WHERE asset = %s AND timeframe = %s AND timestamp = %s
            LIMIT 1
        """, (asset, tf, prev_ts))
        
        row = cur.fetchone()
        
        last_pnl = 0.0
        
        if row:
            prev_signal, prev_price = row
            if prev_signal != 0 and prev_price > 0:
                # Formula: (Current - Last) / Last * Direction
                pct_change = (current_price - prev_price) / prev_price
                last_pnl = pct_change * prev_signal
        
        # 2. Compute 7 Day Rolling Stats
        start_7d = current_ts_dt - timedelta(days=7)
        
        # We need to join signals with their results. 
        # Ideally, we'd have a 'results' table, but we can compute on the fly for the last 7 days.
        # This query fetches all signals in last 7 days to calc accuracy
        # Note: accurate pnl calc requires historical prices. 
        # For simplicity in this worker, we assume we update a 'pnl_realized' column in signals later,
        # or we just push the `last_pnl` into the performance table now.
        
        # Insert the PnL result for this specific turn
        cur.execute("""
            INSERT INTO performance (timestamp, asset, timeframe, last_pnl)
            VALUES (%s, %s, %s, %s)
        """, (current_ts_dt, asset, tf, last_pnl))
        
        # Recalculate 7D aggregates
        cur.execute("""
            SELECT count(*), sum(last_pnl) FROM performance
            WHERE asset = %s AND timeframe = %s 
            AND timestamp >= %s
            AND last_pnl != 0
        """, (asset, tf, start_7d))
        
        stats = cur.fetchone()
        trades_count = stats[0] if stats and stats[0] else 0
        total_pnl = stats[1] if stats and stats[1] else 0.0
        
        # Basic accuracy approximation: count intervals where pnl > 0
        cur.execute("""
            SELECT count(*) FROM performance
            WHERE asset = %s AND timeframe = %s 
            AND timestamp >= %s
            AND last_pnl > 0
        """, (asset, tf, start_7d))
        wins = cur.fetchone()[0]
        accuracy = (wins / trades_count) if trades_count > 0 else 0.0
        
        # Update the row we just inserted with the aggregates
        cur.execute("""
            UPDATE performance 
            SET accuracy_7d = %s, pnl_7d = %s, total_trades_7d = %s
            WHERE asset = %s AND timeframe = %s AND timestamp = %s
        """, (accuracy, total_pnl, trades_count, asset, tf, current_ts_dt))

        conn.commit()
        cur.close()
        
    except Exception as e:
        print(f"[DB Error] Failed to update metrics: {e}")

# =========================================
# 3. DATA & KRAKEN INTEGRATION
# =========================================

def fetch_kraken_ohlc(pair, interval=1, since=None):
    """
    Fetch OHLC from Kraken Public API.
    interval: integer minutes (1, 5, 15, 30, 60, 240, 1440, 10080, 21600)
    """
    url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval={interval}"
    if since:
        url += f"&since={since}"
    
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if data.get('error'):
            print(f"Kraken Error: {data['error']}")
            return []
            
        # Kraken returns a dict where the key is the pair name (variable)
        # We extract the first list value found in 'result'
        res = data['result']
        # The key might be 'XXBTZUSD' or similar, find the list
        candles = []
        last_ts = 0
        for k, v in res.items():
            if isinstance(v, list):
                candles = v
                last_ts = res.get('last', 0)
                break
                
        # Format: [int(time), float(open), float(high), float(low), float(close), ...]
        clean_candles = []
        for c in candles:
            # Kraken time is in seconds, convert to ms if needed, but we keep seconds for Pandas
            clean_candles.append({
                "timestamp": int(c[0]), 
                "price": float(c[4]) # Close price
            })
            
        return clean_candles
    except Exception as e:
        print(f"Fetch Error {pair}: {e}")
        return []

class MarketData:
    def __init__(self):
        self.data_buffer = {} # { "BTCUSDT": DataFrame }

    def initialize_assets(self, assets):
        # Fetch 7 days of 1m history
        print("Initializing 7-day history...")
        now_ts = int(time.time())
        seven_days_ago = now_ts - (7 * 24 * 60 * 60)
        
        for asset in assets:
            kraken_pair = ASSET_MAP.get(asset)
            if not kraken_pair: continue
            
            raw = fetch_kraken_ohlc(kraken_pair, interval=1, since=seven_days_ago)
            if raw:
                df = pd.DataFrame(raw)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df.set_index('timestamp', inplace=True)
                self.data_buffer[asset] = df
                print(f"Loaded {len(df)} candles for {asset}")

    def update(self, asset, timestamp_sec, price):
        """Append new 1m candle"""
        ts = pd.to_datetime(timestamp_sec, unit='s')
        new_row = pd.DataFrame({'price': [price]}, index=[ts])
        
        if asset not in self.data_buffer:
            self.data_buffer[asset] = new_row
        else:
            # Avoid duplicates
            if ts not in self.data_buffer[asset].index:
                self.data_buffer[asset] = pd.concat([self.data_buffer[asset], new_row])
            
            # Keep buffer reasonable (e.g. last 10 days) to prevent RAM explosion
            if len(self.data_buffer[asset]) > 14400: 
                self.data_buffer[asset] = self.data_buffer[asset].iloc[-14400:]

    def get_prices(self, asset, timeframe):
        """Resample 1m buffer to target timeframe and return list of floats"""
        if asset not in self.data_buffer: return []
        
        df = self.data_buffer[asset]
        target_rule = TIMEFRAMES.get(timeframe, "1min")
        
        if target_rule == "1min":
            return df['price'].tolist()
            
        # Resample
        resampled = df['price'].resample(target_rule).last().dropna()
        return resampled.tolist()

# =========================================
# 4. PREDICTION LOGIC (FROM UPLOADED FILE)
# =========================================

class StrategyEngine:
    def __init__(self):
        self.models = {} # { "BTCUSDT_1m": { strategies: [] } }

    def load_models_from_github(self):
        print("Fetching models from GitHub...")
        if not GITHUB_PAT:
            print("WARNING: No GITHUB_PAT. Cannot fetch private models.")
            return

        headers = {"Authorization": f"Bearer {GITHUB_PAT}", "Accept": "application/vnd.github.v3+json"}
        
        # List files
        try:
            r = requests.get(GITHUB_API_URL, headers=headers)
            r.raise_for_status()
            files = r.json()
            
            for file in files:
                fname = file['name']
                if fname.endswith(".json") and "_" in fname:
                    # Download content
                    down_r = requests.get(file['download_url'])
                    model_data = down_r.json()
                    
                    # Store
                    # Structure based on previous script: asset + interval
                    key = fname.replace(".json", "") # e.g. BTCUSDT_1m
                    self.models[key] = model_data.get('strategies', [])
                    print(f"Loaded model: {key}")
                    
        except Exception as e:
            print(f"Error loading models: {e}")

    # --- Reimplemented Logic from your file ---
    def get_bucket(self, price, bucket_size):
        if bucket_size <= 0: return 0
        return int(price // bucket_size)

    def predict(self, asset, timeframe, prices):
        model_key = f"{asset}_{timeframe}"
        strategies = self.models.get(model_key)
        
        if not strategies or len(prices) < 50:
            return 0 # No model or not enough data

        votes = []
        
        for strat in strategies:
            cfg = strat['config']
            params = strat['params']
            
            b_size = params['bucket_size']
            abs_map = params['abs_map']
            der_map = params['der_map']
            
            s_len = cfg['s_len']
            
            # Prepare sequence
            relevant_prices = prices[-(s_len + 1):]
            if len(relevant_prices) < s_len + 1: continue
            
            buckets = [self.get_bucket(p, b_size) for p in relevant_prices]
            
            a_seq_tup = tuple(buckets[:-1])
            last_val = buckets[-1]
            last_price = relevant_prices[-1]
            
            # Helper key gen
            a_key = "|".join(map(str, a_seq_tup))
            d_key = ""
            if s_len > 1:
                d_seq = [buckets[k] - buckets[k-1] for k in range(1, len(buckets))]
                d_key = "|".join(map(str, d_seq))

            # Retrieve predictions
            pred_abs_val = None
            if a_key in abs_map:
                # abs_map structure: {"seq": {"next_val": count}}
                # We need the max count
                candidates = abs_map[a_key]
                pred_abs_val = int(max(candidates, key=candidates.get))

            pred_der_val = None
            if d_key in der_map:
                candidates = der_map[d_key]
                change = int(max(candidates, key=candidates.get))
                pred_der_val = last_val + change

            # Combine logic
            final_pred_bucket = None
            mode = cfg['model']
            
            if mode == "Absolute": final_pred_bucket = pred_abs_val
            elif mode == "Derivative": final_pred_bucket = pred_der_val
            elif mode == "Combined":
                if pred_abs_val is not None and pred_der_val is not None:
                    # Simple consensus direction check
                    dir_a = 1 if pred_abs_val > last_val else -1 if pred_abs_val < last_val else 0
                    dir_d = 1 if pred_der_val > last_val else -1 if pred_der_val < last_val else 0
                    if dir_a == dir_d and dir_a != 0:
                        final_pred_bucket = pred_abs_val # or der, doesn't matter for direction

            if final_pred_bucket is not None:
                diff = final_pred_bucket - last_val
                if diff > 0: votes.append(1)
                elif diff < 0: votes.append(-1)
        
        if not votes: return 0
        
        # Ensemble Vote
        vote_sum = sum(votes)
        if vote_sum > 0: return 1
        elif vote_sum < 0: return -1
        return 0

# =========================================
# 5. MAIN LOOP
# =========================================

def main():
    print("Starting Trader Worker...")
    
    # 1. Initialize
    init_db()
    conn = get_db_connection()
    
    market = MarketData()
    market.initialize_assets(ASSET_MAP.keys())
    
    engine = StrategyEngine()
    engine.load_models_from_github()
    
    # 2. Loop
    print("Entering live loop...")
    while True:
        # Wait for the next minute (at :01 seconds to allow Kraken data to settle)
        now = datetime.now()
        sleep_sec = 60 - now.second + 1
        print(f"Waiting {sleep_sec}s for next candle...")
        time.sleep(sleep_sec)
        
        current_ts = int(time.time())
        current_ts_dt = datetime.fromtimestamp(current_ts)
        
        print(f"\n[{current_ts_dt}] Processing candle...")

        # A. Fetch & Update Data
        for asset, kraken_pair in ASSET_MAP.items():
            # Get latest 1m candle (fetch last 2 to ensure we get the just-closed one)
            candles = fetch_kraken_ohlc(kraken_pair, interval=1)
            if not candles: continue
            
            # The last candle in list is usually open/incomplete on Kraken
            # We want the one before it (the just closed one)
            just_closed = candles[-2] 
            
            # Update market data
            market.update(asset, just_closed['timestamp'], just_closed['price'])
            
            # B. Generate Signals & Update DB
            for tf in TIMEFRAMES.keys():
                # Check if this timeframe closed. 
                # (Simple check: if 1m timestamp is divisible by TF minutes)
                # 1m always runs. 5m runs at :00, :05, etc.
                is_boundary = False
                mins =  (current_ts_dt.minute)
                if tf == "1m": is_boundary = True
                elif tf == "5m" and mins % 5 == 0: is_boundary = True
                elif tf == "15m" and mins % 15 == 0: is_boundary = True
                elif tf == "30m" and mins % 30 == 0: is_boundary = True
                elif tf == "1h" and mins == 0: is_boundary = True
                
                if is_boundary:
                    prices = market.get_prices(asset, tf)
                    
                    # 1. PREDICT (Fast)
                    prediction = engine.predict(asset, tf, prices)
                    
                    # 2. WRITE SIGNAL (Fast)
                    save_signal(conn, asset, tf, prediction, just_closed['price'], current_ts_dt)
                    print(f" >> {asset} [{tf}]: Signal {prediction}")
                    
                    # 3. METRICS (Slow)
                    # We pass the CURRENT price to calculate the result of the PREVIOUS signal
                    update_metrics(conn, asset, tf, just_closed['price'], current_ts_dt)

if __name__ == "__main__":
    main()
