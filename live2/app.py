import os
import json
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# =========================================
# 1. CONFIGURATION
# =========================================

load_dotenv()

GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

DATA_DIR = "./data"
# Specific backtest window
BACKTEST_START = datetime(2026, 1, 1)
BACKTEST_END = datetime(2026, 1, 15)

# =========================================
# 2. MODEL LOADER (Random Selection)
# =========================================

class ModelLoader:
    def __init__(self, pat=None):
        self.headers = {"Authorization": f"Bearer {pat}", "Accept": "application/vnd.github.v3+json"} if pat else {}

    def fetch_random_model(self):
        print(f"[*] Connecting to repository: {REPO_OWNER}/{REPO_NAME}...")
        try:
            r = requests.get(GITHUB_API_URL, headers=self.headers)
            if r.status_code != 200: 
                print(f"❌ Failed to fetch repo content: {r.status_code}")
                return None
            
            files_list = r.json()
            model_files = [f for f in files_list if f['name'].endswith('.json')]
            
            if not model_files:
                print("❌ No JSON model files found.")
                return None
                
            # Random selection
            target_file = random.choice(model_files)
            print(f"[*] Randomly selected file: {target_file['name']}")
            
            if target_file.get('download_url'): 
                return self._download_raw(target_file['download_url'])
        except Exception as e: 
            print(f"❌ Error fetching models: {e}")
            return None

    def _download_raw(self, url):
        try:
            r = requests.get(url, headers=self.headers)
            if r.status_code == 200:
                data = r.json()
                if 'asset' in data: 
                    print(f"    -> Model Loaded for Asset: {data['asset']}")
                    return data
        except Exception as e:
            print(f"❌ Error downloading raw file: {e}")
        return None

# =========================================
# 3. INFERENCE ENGINE (Synchronous)
# =========================================

class InferenceEngine:
    def __init__(self, model_data):
        self.strategies = model_data['strategies']
        self.asset = model_data['asset']
        self.interval = model_data['interval']
        
        # Parse duration (e.g., '15m' -> 15)
        self.duration_min = 1
        if 'm' in self.interval: 
            self.duration_min = int(self.interval.replace('m', ''))
        elif 'h' in self.interval: 
            self.duration_min = int(self.interval.replace('h', '')) * 60

    def _get_bucket(self, price, bucket_size):
        return int(price // bucket_size) if bucket_size > 0 else 0

    def _lookup(self, map_data, key):
        return map_data.get(key)

    def predict(self, recent_prices):
        active_signals = []
        
        # We need at least enough history for the strategies
        current_price = recent_prices[-1]

        for strat in self.strategies:
            cfg = strat['config']
            params = strat['params']
            s_len = cfg['s_len']
            b_size = params['bucket_size']
            
            if len(recent_prices) < s_len + 1: continue

            window = recent_prices[-s_len:]
            buckets = [self._get_bucket(p, b_size) for p in window]
            last_bucket = buckets[-1]
            
            a_seq = "|".join(map(str, buckets))
            d_seq = "|".join(map(str, [buckets[i]-buckets[i-1] for i in range(1,len(buckets))])) if s_len > 1 else ""

            pred = None
            if cfg['model'] == "Absolute": 
                pred = self._lookup(params['abs_map'], a_seq)
            elif cfg['model'] == "Derivative":
                chg = self._lookup(params['der_map'], d_seq)
                if chg: pred = last_bucket + chg
            elif cfg['model'] == "Combined":
                p1 = self._lookup(params['abs_map'], a_seq)
                chg = self._lookup(params['der_map'], d_seq)
                p2 = last_bucket + chg if chg else None
                
                d1 = 0 if p1 is None else (1 if p1 > last_bucket else -1 if p1 < last_bucket else 0)
                d2 = 0 if p2 is None else (1 if p2 > last_bucket else -1 if p2 < last_bucket else 0)
                
                if d1!=0 and d1==d2: pred = p1
                elif d1!=0 and d2==0: pred = p1
                elif d2!=0 and d1==0: pred = p2

            if pred is not None:
                diff = pred - last_bucket
                if diff != 0:
                    active_signals.append({
                        "dir": 1 if diff > 0 else -1,
                        "b_count": cfg['b_count'],
                        "est_price": pred * b_size,
                        "bucket_size": b_size
                    })

        if not active_signals: return 0, 0
        
        # Filter for consistency
        directions = {x['dir'] for x in active_signals}
        if len(directions) > 1: return 0, 0
            
        active_signals.sort(key=lambda x: x['b_count'])
        return active_signals[0]['dir'], active_signals[0]['bucket_size']

# =========================================
# 4. DATA MANAGER
# =========================================

def get_data(symbol, start_date, end_date):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    file_path = f"{DATA_DIR}/{symbol}_1m.csv"
    
    # 1. Try to load local
    if os.path.exists(file_path):
        print(f"[*] Found local data: {file_path}")
        df = pd.read_csv(file_path)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df
    
    # 2. Download if missing (Download 2 weeks around the target date)
    print(f"[*] Local data missing. Downloading {symbol} from Binance...")
    
    # Buffer: 5 days before start, 2 days after end to be safe
    dl_start = int((start_date - timedelta(days=5)).timestamp() * 1000)
    dl_end = int((end_date + timedelta(days=2)).timestamp() * 1000)
    
    all_candles = []
    current_start = dl_start
    
    while current_start < dl_end:
        url = f"{BINANCE_API_URL}?symbol={symbol}&interval=1m&limit=1000&startTime={current_start}"
        try:
            r = requests.get(url)
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            
            for x in data:
                all_candles.append({
                    "datetime": datetime.fromtimestamp(x[0]/1000),
                    "open": float(x[1]),
                    "high": float(x[2]),
                    "low": float(x[3]),
                    "close": float(x[4])
                })
            current_start = data[-1][0] + 60000 # +1 min
        except Exception as e:
            print(e)
            break
            
    if not all_candles:
        print("❌ Failed to download data.")
        return pd.DataFrame()
        
    df = pd.DataFrame(all_candles)
    print(f"    -> Saving {len(df)} candles to {file_path}")
    df.to_csv(file_path, index=False)
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    return df

# =========================================
# 5. MAIN BACKTEST LOOP
# =========================================

def run():
    # A. Fetch Model
    loader = ModelLoader(pat=GITHUB_PAT)
    model_data = loader.fetch_random_model()
    if not model_data: return
    
    engine = InferenceEngine(model_data)
    asset = engine.asset
    
    # B. Get Data
    df_all = get_data(asset, BACKTEST_START, BACKTEST_END)
    if df_all.empty: return

    # Filter strictly for Jan 1 - Jan 15 (plus warmup buffer)
    # We need data BEFORE Jan 1 for the model to calculate the first signals
    mask = (df_all.index >= (BACKTEST_START - timedelta(hours=5))) & (df_all.index <= BACKTEST_END)
    df = df_all.loc[mask].sort_index()
    
    print(f"\n--- 🧪 RUNNING BACKTEST: {asset} ---")
    print(f"Period: {BACKTEST_START} -> {BACKTEST_END}")
    print(f"Candles: {len(df)}")
    
    # C. Simulation State
    active_trade = None
    trades = []
    
    history = []
    
    for ts, row in df.iterrows():
        close = row['close']
        high = row['high']
        low = row['low']
        history.append(close)
        
        # 1. Manage Active Trade
        if active_trade:
            res = "OPEN"
            exit_price = close
            
            # Check stops
            is_win = False
            is_loss = False
            
            if active_trade['dir'] == 1: # LONG
                if high >= active_trade['tp']: is_win = True
                if low <= active_trade['sl']: is_loss = True
            else: # SHORT
                if low <= active_trade['tp']: is_win = True
                if high >= active_trade['sl']: is_loss = True
            
            if is_loss: 
                res = "LOSS"
                exit_price = active_trade['sl']
            elif is_win: 
                res = "WIN"
                exit_price = active_trade['tp']
            elif ts >= active_trade['expiry']:
                res = "EXPIRED"
                exit_price = close
                
            if res != "OPEN":
                # Calc PnL
                raw_pnl = (exit_price - active_trade['entry']) * active_trade['dir']
                pnl_pct = (raw_pnl / active_trade['entry']) * 100
                
                trades.append({
                    "time": ts,
                    "type": res,
                    "pnl": pnl_pct
                })
                active_trade = None

        # 2. Check for New Signal (Only if no active trade and inside window)
        if not active_trade and ts >= BACKTEST_START:
            direction, b_size = engine.predict(history)
            
            if direction != 0:
                tp = close + (b_size * direction)
                sl = close - (b_size * direction)
                expiry = ts + timedelta(minutes=engine.duration_min)
                
                active_trade = {
                    "entry": close,
                    "dir": direction,
                    "tp": tp,
                    "sl": sl,
                    "expiry": expiry
                }

    # D. Stats
    wins = len([t for t in trades if t['pnl'] > 0])
    losses = len([t for t in trades if t['pnl'] <= 0])
    total = len(trades)
    accuracy = (wins/total*100) if total > 0 else 0
    cum_pnl = sum([t['pnl'] for t in trades])
    
    print("\n" + "="*30)
    print("📊 FINAL RESULTS")
    print("="*30)
    print(f"Asset:      {asset}")
    print(f"Trades:     {total}")
    print(f"Wins:       {wins}")
    print(f"Losses:     {losses}")
    print(f"Accuracy:   {accuracy:.2f}%")
    print(f"Total PnL:  {cum_pnl:.2f}%")
    print("="*30)

if __name__ == "__main__":
    run()
