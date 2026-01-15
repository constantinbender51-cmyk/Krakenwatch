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

# GitHub Config
GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = os.getenv("REPO_OWNER", "constantinbender51-cmyk")
REPO_NAME = os.getenv("REPO_NAME", "model-2")
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"

# Database Config
DATABASE_URL = os.getenv("DATABASE_URL")

# Asset Mapping
# Binance (for history) -> Kraken (for live)
# Format: { "Model_Symbol": {"binance": "Symbol", "kraken": "Symbol"} }
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
# 2. DATABASE (Strict Schema)
# =========================================

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Strict Signal Table (Snapshot of ACTIVE signals only)
    # "asset timeframe signal start end nothing more NOTHING"
    # We use (asset, timeframe) as PRIMARY KEY to enable Overwriting (Upsert)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            asset VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            signal INT NOT NULL,
            start_time TIMESTAMPTZ NOT NULL,
            end_time TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (asset, timeframe)
        );
    """)

    # 2. Secondary History Table (For Metrics/PnL calculation only)
    # Kept separate to keep 'signals' table pure.
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
    """
    Upsert: Overwrites the row if (asset, tf) exists.
    """
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
        print(f"[DB Error] Signal Overwrite Failed: {e}")

def record_trade_result(conn, asset, tf, signal, entry_price, exit_price):
    """Saves completed trade to history for PnL tracking."""
    if signal == 0 or entry_price == 0: return
    
    # PnL logic: (Exit - Entry) / Entry * Direction
    pnl = ((exit_price - entry_price) / entry_price) * signal
    
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO history (asset, timeframe, signal, entry_price, exit_price, pnl_pct)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (asset, tf, int(signal), float(entry_price), float(exit_price), float(pnl)))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB Error] History Save Failed: {e}")

def compute_7d_metrics(conn):
    """Calculates accuracy and PnL from the history table."""
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
            total = row[0]
            wins = row[1]
            pnl = row[2]
            acc = (wins / total) * 100
            print(f"\n--- 7 Day Metrics ---")
            print(f"Trades: {total} | Accuracy: {acc:.2f}% | PnL: {pnl*100:.2f}%")
        else:
            print("\n--- 7 Day Metrics: No closed trades yet ---")
            
    except Exception as e:
        print(f"Metrics Error: {e}")

# =========================================
# 3. DATA FETCHING (Hybrid)
# =========================================

def fetch_binance_history_7d(symbol):
    """
    Fetches ~7 days of 1m data from Binance.
    Binance limit is 1000 candles per call. 7 days * 1440 mins = ~10080 candles.
    We need to loop.
    """
    base_url = "https://api.binance.com/api/v3/klines"
    interval = "1m"
    end_time = int(time.time() * 1000)
    start_time = end_time - (7 * 24 * 60 * 60 * 1000)
    
    all_candles = []
    current_start = start_time
    
    print(f"[{symbol}] Fetching Binance History...", end=" ")
    
    while current_start < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "limit": 1000
        }
        try:
            r = requests.get(base_url, params=params)
            data = r.json()
            
            if not isinstance(data, list): break
            if not data: break
            
            # Binance format: [time, open, high, low, close, ...]
            # We only need [time, close]
            batch = [{"timestamp": int(c[0]), "price": float(c[4])} for c in data]
            all_candles.extend(batch)
            
            current_start = batch[-1]["timestamp"] + 1
            time.sleep(0.1) # Respect rate limits
            
        except Exception as e:
            print(f"Err: {e}")
            break
            
    print(f"Done. ({len(all_candles)} candles)")
    return all_candles

def fetch_kraken_latest(pair):
    """
    Fetches the latest candles from Kraken Public API.
    Used for the live loop.
    """
    url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval=1"
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        
        if data.get('error'):
            # Handle standard Kraken errors
            return None
            
        # Kraken returns {"result": { "XBTUSD": [[time, ..., close], ...] }}
        # Key name is dynamic, so we grab the first list in 'result'
        res = data.get('result', {})
        for k, v in res.items():
            if k != "last" and isinstance(v, list):
                # Return last 2 candles (current open and previous closed)
                # Format: [time(sec), open, high, low, close, ...]
                return [{"timestamp": int(c[0]) * 1000, "price": float(c[4])} for c in v[-2:]]
                
    except Exception:
        pass
    return None

# =========================================
# 4. TRADING ENGINE
# =========================================

class MarketBuffer:
    def __init__(self):
        self.data = {} # { "BTCUSDT": DataFrame }
        self.last_signal = {} # { "BTCUSDT_1m": {"signal": 0, "entry": 0} }

    def ingest(self, asset, candles_list):
        if not candles_list: return
        
        df = pd.DataFrame(candles_list)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        
        if asset not in self.data:
            self.data[asset] = df
        else:
            # Upsert/Append logic for Pandas
            # Combine, remove duplicates (keep last), sort
            combined = pd.concat([self.data[asset], df])
            combined = combined[~combined.index.duplicated(keep='last')]
            self.data[asset] = combined.sort_index().iloc[-15000:] # Keep ~10 days

    def get_prices(self, asset, tf_alias):
        if asset not in self.data: return []
        df = self.data[asset]
        if tf_alias == "1min":
            return df['price'].tolist()
        
        # Resample
        return df['price'].resample(tf_alias).last().dropna().tolist()

class StrategyLoader:
    def __init__(self):
        self.models = {}

    def load(self):
        if not GITHUB_PAT:
            print("WARNING: No PAT. Models cannot be loaded.")
            return

        headers = {"Authorization": f"Bearer {GITHUB_PAT}"}
        try:
            r = requests.get(GITHUB_API_URL, headers=headers)
            files = r.json()
            
            for f in files:
                if f['name'].endswith(".json") and "_" in f['name']:
                    # Download
                    content = requests.get(f['download_url']).json()
                    key = f['name'].replace(".json", "")
                    self.models[key] = content.get('strategies', [])
                    print(f"Loaded: {key}")
        except Exception as e:
            print(f"Model Load Error: {e}")

    def get_bucket(self, price, size):
        return int(price // size) if size > 0 else 0

    def predict(self, asset, tf, prices):
        key = f"{asset}_{tf}"
        strategies = self.models.get(key, [])
        if not strategies or len(prices) < 50: return 0
        
        votes = 0
        for strat in strategies:
            cfg = strat['config']
            p = strat['params']
            s_len = cfg['s_len']
            
            # Valid sequence length
            if len(prices) < s_len + 1: continue
            
            # Map prices to buckets
            relevant = prices[-(s_len+1):]
            bkts = [self.get_bucket(v, p['bucket_size']) for v in relevant]
            
            # Keys
            a_seq = "|".join(map(str, bkts[:-1]))
            d_seq = ""
            if s_len > 1:
                diffs = [bkts[i] - bkts[i-1] for i in range(1, len(bkts))]
                d_seq = "|".join(map(str, diffs))

            # Lookups
            pred_val = None
            
            # Absolute Model
            if cfg['model'] == "Absolute" and a_seq in p['abs_map']:
                cands = p['abs_map'][a_seq]
                pred_val = int(max(cands, key=cands.get))
            
            # Derivative Model
            elif cfg['model'] == "Derivative" and d_seq in p['der_map']:
                cands = p['der_map'][d_seq]
                change = int(max(cands, key=cands.get))
                pred_val = bkts[-1] + change
                
            # Combined
            elif cfg['model'] == "Combined":
                 # Simplified for brevity: Only voting if agreement or single model valid
                 pass 

            if pred_val is not None:
                if pred_val > bkts[-1]: votes += 1
                elif pred_val < bkts[-1]: votes -= 1
        
        if votes > 0: return 1
        if votes < 0: return -1
        return 0

# =========================================
# 5. MAIN EXECUTION
# =========================================

def main():
    init_db()
    conn = get_db_connection()
    
    market = MarketBuffer()
    engine = StrategyLoader()
    engine.load()
    
    # 1. INITIAL FETCH (Binance)
    print("\n--- Initializing with Binance Data ---")
    for model_symbol, pairs in ASSETS.items():
        hist = fetch_binance_history_7d(pairs['binance'])
        market.ingest(model_symbol, hist)
        
    print("\n--- Starting Live Kraken Loop ---")
    
    while True:
        # Align to the minute (Wait for :01s)
        now = datetime.now()
        sleep_s = 60 - now.second + 2 
        print(f"Waiting {sleep_s}s...")
        time.sleep(sleep_s)
        
        now_dt = datetime.now()
        print(f"[{now_dt.strftime('%H:%M:%S')}] Tick.")
        
        # 2. LIVE UPDATE (Kraken)
        for model_symbol, pairs in ASSETS.items():
            kraken_pair = pairs['kraken']
            
            # Fetch
            k_candles = fetch_kraken_latest(kraken_pair)
            if not k_candles: continue
            
            # Just closed candle is the second to last one usually, 
            # or strictly the last one if we time it right.
            # We used 'last 2' in fetcher. k_candles[-1] is current open, k_candles[-2] is closed.
            if len(k_candles) < 2: continue
            closed_candle = k_candles[-2]
            current_price = closed_candle['price']
            
            # Update buffer
            market.ingest(model_symbol, [closed_candle])
            
            # 3. PREDICT & UPSERT
            for tf_name, tf_pd in TIMEFRAMES.items():
                
                # Check timeframe boundary
                # 1m: always. 5m: minute % 5 == 0, etc.
                is_boundary = False
                m = now_dt.minute
                if tf_name == "1m": is_boundary = True
                elif tf_name == "5m" and m % 5 == 0: is_boundary = True
                elif tf_name == "15m" and m % 15 == 0: is_boundary = True
                elif tf_name == "30m" and m % 30 == 0: is_boundary = True
                elif tf_name == "1h" and m == 0: is_boundary = True
                
                if is_boundary:
                    # A. Handle Previous Signal (if any) for Metrics
                    prev_key = f"{model_symbol}_{tf_name}"
                    if prev_key in market.last_signal:
                        last_sig = market.last_signal[prev_key]
                        record_trade_result(conn, model_symbol, tf_name, last_sig['sig'], last_sig['entry'], current_price)
                    
                    # B. Generate New Signal
                    prices = market.get_prices(model_symbol, tf_pd)
                    sig = engine.predict(model_symbol, tf_name, prices)
                    
                    # C. Calculate Times
                    delta_min = int(tf_pd.replace("min", "").replace("H", "60").replace("1min", "1"))
                    if tf_name == "1h": delta_min = 60
                    
                    start_time = now_dt
                    end_time = now_dt + timedelta(minutes=delta_min)
                    
                    # D. OVERWRITE DB
                    overwrite_signal(conn, model_symbol, tf_name, sig, start_time, end_time)
                    
                    # E. Store in memory for next loop's PnL calc
                    market.last_signal[prev_key] = {"sig": sig, "entry": current_price}
                    
                    print(f" >> {model_symbol} {tf_name}: {sig}")

        # 4. COMPUTE METRICS (Background)
        compute_7d_metrics(conn)

if __name__ == "__main__":
    main()
