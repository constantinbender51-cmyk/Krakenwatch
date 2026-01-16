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
# 2. STATS & TRADE TRACKING (With Worst-Case Logic)
# =========================================

class TradeTracker:
    def __init__(self):
        self.active_trades = []
        self.closed_trades = []
        self.score = 0.0
        self.pnl_cumulative = 0.0
        
        self.hits_in_direction = 0
        self.total_significant_moves = 0

    def add_trade(self, asset, direction, entry_price, bucket_size, duration_minutes, interval):
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
            "start_time": datetime.now()
        }
        self.active_trades.append(trade)
        return trade

    def update(self, asset, current_close, current_high, current_low):
        asset_trades = [t for t in self.active_trades if t['asset'] == asset]
        
        for trade in asset_trades:
            is_closed = False
            result = "OPEN"
            exit_price = current_close
            
            hit_win = False
            hit_loss = False
            
            # 1. Check Levels
            if trade['direction'] == 1: # LONG
                if current_high >= trade['target_price']: hit_win = True
                if current_low <= trade['stop_price']: hit_loss = True
            
            elif trade['direction'] == -1: # SHORT
                if current_low <= trade['target_price']: hit_win = True
                if current_high >= trade['stop_price']: hit_loss = True

            # 2. Resolve Ambiguity (The Worst-Case Rule)
            if hit_win and hit_loss:
                # Candle covered both -> Assume STOP hit first
                is_closed = True
                result = "LOSS_HIT"
                exit_price = trade['stop_price']
            elif hit_loss:
                is_closed = True
                result = "LOSS_HIT"
                exit_price = trade['stop_price']
            elif hit_win:
                is_closed = True
                result = "WIN_HIT"
                exit_price = trade['target_price']

            # 3. Check Expiry
            if not is_closed and datetime.now() >= trade['expiry']:
                is_closed = True
                result = "EXPIRED"
                exit_price = current_close

            if is_closed:
                self._finalize_trade(trade, exit_price, result)

    def _finalize_trade(self, trade, exit_price, result):
        pnl = (exit_price - trade['entry_price']) * trade['direction']
        self.pnl_cumulative += pnl
        self.score += pnl
        
        moved_bucket = False
        won_bucket = False
        
        if result == "WIN_HIT":
            moved_bucket = True
            won_bucket = True
        elif result == "LOSS_HIT":
            moved_bucket = True
            won_bucket = False
        elif result == "EXPIRED":
            # Only count for accuracy stats if it moved significantly
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
            "reason": result,
            "moved_bucket": moved_bucket
        })

    def get_stats(self):
        acc = 0.0
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
# 3. DATA & MODEL UTILS
# =========================================

class ModelLoader:
    def __init__(self, pat=None):
        self.headers = {"Authorization": f"Bearer {pat}", "Accept": "application/vnd.github.v3+json"} if pat else {}

    def fetch_all_models(self):
        print(f"[*] Scanning repository: {REPO_OWNER}/{REPO_NAME}...")
        try:
            r = requests.get(GITHUB_API_URL, headers=self.headers)
            if r.status_code != 200: return []
            
            files_list = r.json()
            if not isinstance(files_list, list): return []
            
            model_files = [f for f in files_list if f['name'].endswith('.json')]
            print(f"[*] Found {len(model_files)} files. Downloading large files...")
            
            loaded = []
            for f in model_files:
                if f.get('download_url'): 
                    self._download_raw(f['download_url'], f['name'], loaded)
            return loaded
        except Exception as e: 
            print(f"Error loading models: {e}")
            return []

    def _download_raw(self, url, filename, container):
        try:
            r = requests.get(url, headers=self.headers, stream=True)
            if r.status_code == 200:
                data = r.json()
                if 'asset' in data: 
                    container.append(data)
                    print(f"    -> Loaded: {data['asset']:<10} | {len(r.content)/1e6:.2f} MB")
        except: pass

class InferenceEngine:
    def __init__(self, model_data):
        self.strategies = model_data['strategies']
        self.asset = model_data['asset']
        self.interval = model_data['interval']
        self.duration_min = 1
        if 'm' in self.interval: self.duration_min = int(self.interval.replace('m', ''))
        elif 'h' in self.interval: self.duration_min = int(self.interval.replace('h', '')) * 60

    def _get_bucket(self, price, bucket_size):
        return int(price // bucket_size) if bucket_size > 0 else 0

    def _lookup(self, map_data, key):
        return map_data.get(key)

    def predict(self, recent_prices):
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
        return active_signals[0]['dir'], active_signals[0]['est_price'], active_signals[0]['bucket_size']

# =========================================
# 4. ASYNC LOOP
# =========================================

async def fetch_candle_data(session, engine):
    url = f"{BINANCE_API_URL}?symbol={engine.asset}&interval={engine.interval}&limit=50"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                closes = [float(x[4]) for x in data]
                high = float(data[-1][2]) 
                low = float(data[-1][3]) 
                return engine, closes, high, low
    except: pass
    return engine, [], 0, 0

async def run_bot_loop(engines):
    tracker = TradeTracker()
    print(f"\n🚀 BOT LIVE: Safe Mode (Worst-Case Assumption Active)\n")

    async with aiohttp.ClientSession() as session:
        while True:
            now = datetime.now()
            # Calculate sleep to wake up at XX:XX:00.1
            sleep_sec = 60 - now.second - (now.microsecond/1e6) + 0.1
            if sleep_sec < 0: sleep_sec += 60
            
            print(f"⏳ Waiting {sleep_sec:.1f}s...")
            await asyncio.sleep(sleep_sec)

            start_ts = datetime.now()
            print(f"\n--- ⚡ {start_ts.strftime('%H:%M:%S')} ---")

            tasks = [fetch_candle_data(session, eng) for eng in engines]
            results = await asyncio.gather(*tasks)
            signals = []
            
            for engine, closes, high, low in results:
                if not closes: continue
                current_price = closes[-1]
                
                tracker.update(engine.asset, current_price, high, low)
                
                direction, target, b_size = engine.predict(closes)
                if direction != 0:
                    tracker.add_trade(engine.asset, direction, current_price, b_size, engine.duration_min, engine.interval)
                    signals.append({
                        "asset": engine.asset,
                        "dir": direction,
                        "price": current_price,
                        "target": target,
                        "valid": f"{engine.duration_min}m"
                    })

            stats = tracker.get_stats()
            print(f"📊 ACCURACY: {stats['accuracy']:.1f}% ({stats['denominator']} moves) | PnL: {stats['pnl']:.4f}")
            
            if signals:
                print(f"{'ASSET':<10} {'DIR':<6} {'PRICE':<10} {'TARGET':<10} {'VALID'}")
                print("-" * 50)
                for s in signals:
                    d_str = "BUY 🟢" if s['dir']==1 else "SELL 🔴"
                    print(f"{s['asset']:<10} {d_str:<6} {s['price']:<10.4f} {s['target']:<10.4f} {s['valid']}")
            else:
                print("(No new signals)")
            
            latency = (datetime.now()-start_ts).total_seconds()*1000
            print(f"Latency: {latency:.0f}ms")

# =========================================
# 5. MAIN
# =========================================

def main():
    loader = ModelLoader(pat=GITHUB_PAT)
    raw = loader.fetch_all_models()
    
    if not raw: 
        print("No models found. Exiting.")
        return
        
    engines = [InferenceEngine(m) for m in raw]
    
    try:
        asyncio.run(run_bot_loop(engines))
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
