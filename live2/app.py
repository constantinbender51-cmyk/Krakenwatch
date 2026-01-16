import os
import json
import asyncio
import aiohttp
import requests
import time
from datetime import datetime, timedelta

# =========================================
# 1. CONFIGURATION
# =========================================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

GITHUB_PAT = os.getenv("PAT")
REPO_OWNER = "constantinbender51-cmyk"
REPO_NAME = "model-2"
GITHUB_API_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/"
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# =========================================
# 2. STATS & TRADE TRACKING
# =========================================

class TradeTracker:
    def __init__(self):
        self.active_trades = []  # List of dicts
        self.closed_trades = []
        self.score = 0
        self.pnl_cumulative = 0.0
        
        # Accuracy Counters
        self.hits_in_direction = 0
        self.total_significant_moves = 0  # Denominator (moved > bucket)

    def add_trade(self, asset, direction, entry_price, bucket_size, duration_minutes, interval):
        # Determine expiry time
        expiry = datetime.now() + timedelta(minutes=duration_minutes)
        
        trade = {
            "asset": asset,
            "direction": direction,
            "entry_price": entry_price,
            "bucket_size": bucket_size,
            "target_price": entry_price + (bucket_size * direction),
            "stop_price": entry_price - (bucket_size * direction),
            "expiry": expiry,
            "interval": interval,
            "start_time": datetime.now(),
            "max_pnl": 0
        }
        self.active_trades.append(trade)
        return trade

    def update(self, asset, current_close, current_high, current_low):
        # Filter trades for this asset
        asset_trades = [t for t in self.active_trades if t['asset'] == asset]
        
        for trade in asset_trades:
            # 1. Check if hit Target or Stop (using High/Low of the candle)
            # We assume worst-case for stop (check stop first) or best-case? 
            # Standard backtest: check if Low < Stop (for Long) -> Stop Hit.
            
            is_closed = False
            result = "OPEN"
            exit_price = current_close
            
            # LONG
            if trade['direction'] == 1:
                if current_low <= trade['stop_price']:
                    is_closed = True
                    result = "LOSS_HIT"
                    exit_price = trade['stop_price']
                elif current_high >= trade['target_price']:
                    is_closed = True
                    result = "WIN_HIT"
                    exit_price = trade['target_price']
            
            # SHORT
            elif trade['direction'] == -1:
                if current_high >= trade['stop_price']:
                    is_closed = True
                    result = "LOSS_HIT"
                    exit_price = trade['stop_price']
                elif current_low <= trade['target_price']:
                    is_closed = True
                    result = "WIN_HIT"
                    exit_price = trade['target_price']

            # 2. Check Expiry (if not already hit)
            if not is_closed and datetime.now() >= trade['expiry']:
                is_closed = True
                result = "EXPIRED"
                exit_price = current_close

            if is_closed:
                self._finalize_trade(trade, exit_price, result)

    def _finalize_trade(self, trade, exit_price, reason):
        pnl = (exit_price - trade['entry_price']) * trade['direction']
        self.pnl_cumulative += pnl
        self.score += pnl  # "Total Score"
        
        # Accuracy Logic: 
        # "Record accuracy = times price moved at least bucketsize in direction / times price moved at least bucketsize"
        
        # Did price move at least bucket size in ANY direction?
        # We know this if reason is WIN_HIT or LOSS_HIT.
        # If reason is EXPIRED, we check the difference manually.
        
        moved_bucket = False
        won_bucket = False
        
        if reason == "WIN_HIT":
            moved_bucket = True
            won_bucket = True
        elif reason == "LOSS_HIT":
            moved_bucket = True
            won_bucket = False
        elif reason == "EXPIRED":
            # Check if the final move was big enough to count for the denominator
            if abs(exit_price - trade['entry_price']) >= trade['bucket_size']:
                moved_bucket = True
                if pnl > 0: won_bucket = True

        if moved_bucket:
            self.total_significant_moves += 1
            if won_bucket:
                self.hits_in_direction += 1

        self.active_trades.remove(trade)
        self.closed_trades.append({
            "asset": trade['asset'],
            "pnl": pnl,
            "reason": reason,
            "moved_bucket": moved_bucket
        })
        
        # print(f"    [Trade Closed] {trade['asset']} ({trade['interval']}) | PnL: {pnl:.4f} | {reason}")

    def get_stats(self):
        acc = 0
        if self.total_significant_moves > 0:
            acc = (self.hits_in_direction / self.total_significant_moves) * 100
            
        return {
            "accuracy": acc,
            "pnl": self.pnl_cumulative,
            "active": len(self.active_trades),
            "score": self.score,
            "denominator": self.total_significant_moves
        }

# =========================================
# 3. MODEL LOADER & ENGINE (Same as before)
# =========================================

class ModelLoader:
    def __init__(self, pat=None):
        self.headers = {"Authorization": f"Bearer {pat}", "Accept": "application/vnd.github.v3+json"} if pat else {}

    def fetch_all_models(self):
        print(f"[*] Scanning repository: {REPO_OWNER}/{REPO_NAME}...")
        try:
            r = requests.get(GITHUB_API_URL, headers=self.headers)
            if r.status_code != 200: return []
            model_files = [f for f in r.json() if f['name'].endswith('.json')]
            print(f"[*] Found {len(model_files)} files. Downloading...")
            loaded = []
            for f in model_files:
                if f.get('download_url'): self._download_raw(f['download_url'], f['name'], loaded)
            return loaded
        except Exception: return []

    def _download_raw(self, url, filename, container):
        try:
            r = requests.get(url, headers=self.headers, stream=True)
            if r.status_code == 200:
                data = r.json()
                if 'asset' in data: container.append(data)
                print(f"    -> Loaded: {data['asset']:<10} | {len(r.content)/1e6:.2f} MB")
        except: pass

class InferenceEngine:
    def __init__(self, model_data):
        self.strategies = model_data['strategies']
        self.asset = model_data['asset']
        self.interval = model_data['interval']
        
        # Map interval string to minutes for validity checking
        # '1m' -> 1, '5m' -> 5, '1h' -> 60
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
        # Expecting recent_prices to be CLOSES
        active_signals = []
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

        if not active_signals: return 0, current_price, 0
        
        directions = {x['dir'] for x in active_signals}
        if len(directions) > 1: return 0, current_price, 0
            
        active_signals.sort(key=lambda x: x['b_count'])
        winner = active_signals[0]
        return winner['dir'], winner['est_price'], winner['bucket_size']

# =========================================
# 4. ASYNC LOOP
# =========================================

async def fetch_candle_data(session, engine):
    """Fetches Close, High, Low for stats tracking."""
    url = f"{BINANCE_API_URL}?symbol={engine.asset}&interval={engine.interval}&limit=50"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                # Data format: [time, open, high, low, close, ...]
                # We need Closes for prediction, and High/Low for trade checking
                closes = [float(x[4]) for x in data]
                high = float(data[-1][2]) # High of LAST candle
                low = float(data[-1][3])  # Low of LAST candle
                return engine, closes, high, low
    except: pass
    return engine, [], 0, 0

async def run_bot_loop(engines):
    tracker = TradeTracker()
    
    print("\n" + "="*60)
    print(f"🚀 BOT LIVE: PnL & Accuracy Tracking Enabled")
    print("="*60 + "\n")

    async with aiohttp.ClientSession() as session:
        while True:
            # Sync to next second :00
            now = datetime.now()
            sleep_sec = 60 - now.second - (now.microsecond/1e6) + 0.1
            if sleep_sec < 0: sleep_sec += 60
            print(f"⏳ Waiting {sleep_sec:.1f}s...")
            await asyncio.sleep(sleep_sec)

            start_ts = datetime.now()
            print(f"\n--- ⚡ UPDATE: {start_ts.strftime('%H:%M:%S')} ---")

            # 1. Fetch Data
            tasks = [fetch_candle_data(session, eng) for eng in engines]
            results = await asyncio.gather(*tasks)

            # 2. Update Existing Trades & Generate New Signals
            signals = []
            
            for engine, closes, high, low in results:
                if not closes: continue
                
                current_price = closes[-1]
                
                # A) Update Active Trades for this asset
                # We use the High/Low of the candle that JUST closed (index -1)
                tracker.update(engine.asset, current_price, high, low)
                
                # B) Predict Next Move
                direction, target, b_size = engine.predict(closes)
                
                if direction != 0:
                    # Register new trade
                    # Entry is current Close
                    tracker.add_trade(engine.asset, direction, current_price, b_size, engine.duration_min, engine.interval)
                    
                    signals.append({
                        "asset": engine.asset,
                        "dir": direction,
                        "price": current_price,
                        "target": target,
                        "valid": f"{engine.duration_min}m"
                    })

            # 3. Display Dashboard
            stats = tracker.get_stats()
            print(f"\n📊 ACCURACY: {stats['accuracy']:.1f}% ({stats['denominator']} sig moves)")
            print(f"💰 SCORE/PnL: {stats['pnl']:.4f} | ACTIVE: {stats['active']}")
            
            if signals:
                print(f"\n{'ASSET':<10} {'DIR':<6} {'ENTRY':<10} {'TARGET':<10} {'VALID'}")
                print("-" * 50)
                for s in signals:
                    d_str = "BUY 🟢" if s['dir']==1 else "SELL 🔴"
                    print(f"{s['asset']:<10} {d_str:<6} {s['price']:<10.4f} {s['target']:<10.4f} {s['valid']}")
            else:
                print("\n(No new signals this interval)")
                
            print(f"\nLatency: {(datetime.now()-start_ts).total_seconds()*1000:.0f}ms")

# =========================================
# 5. MAIN
# =========================================

def main():
    loader = ModelLoader(pat=GITHUB_PAT)
    raw = loader.fetch_all_models()
    if not raw: return
    
    engines = [InferenceEngine(m) for m in raw]
    try:
        asyncio.run(run_bot_loop(engines))
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
