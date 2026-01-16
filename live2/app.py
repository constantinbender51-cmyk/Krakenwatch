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

# Backtest Settings
BACKTEST_DAYS = 7

# =========================================
# 2. STATS & TRADE TRACKING
# =========================================

class TradeTracker:
    def __init__(self):
        self.active_trades = []
        self.closed_trades = []
        self.score = 0.0
        self.pnl_cumulative = 0.0
        self.hits_in_direction = 0
        self.total_significant_moves = 0

    def add_trade(self, asset, direction, entry_price, bucket_size, duration_minutes, interval, start_time):
        expiry = start_time + timedelta(minutes=duration_minutes)
        trade = {
            "asset": asset,
            "direction": direction,
            "entry_price": entry_price,
            "bucket_size": bucket_size,
            "target_price": entry_price + (bucket_size * direction),
            "stop_price": entry_price - (bucket_size * direction),
            "expiry": expiry,
            "interval": interval,
            "start_time": start_time
        }
        self.active_trades.append(trade)

    def update(self, asset, current_close, current_high, current_low, current_time):
        # Filter trades for this asset
        asset_trades = [t for t in self.active_trades if t['asset'] == asset]
        
        for trade in asset_trades:
            is_closed = False
            result = "OPEN"
            exit_price = current_close
            
            hit_win = False
            hit_loss = False
            
            # 1. Check Levels (High/Low of current candle)
            if trade['direction'] == 1: # LONG
                if current_high >= trade['target_price']: hit_win = True
                if current_low <= trade['stop_price']: hit_loss = True
            elif trade['direction'] == -1: # SHORT
                if current_low <= trade['target_price']: hit_win = True
                if current_high >= trade['stop_price']: hit_loss = True

            # 2. Worst-Case Resolution
            if hit_win and hit_loss:
                is_closed = True; result = "LOSS_HIT"; exit_price = trade['stop_price']
            elif hit_loss:
                is_closed = True; result = "LOSS_HIT"; exit_price = trade['stop_price']
            elif hit_win:
                is_closed = True; result = "WIN_HIT"; exit_price = trade['target_price']

            # 3. Check Expiry
            if not is_closed and current_time >= trade['expiry']:
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
            moved_bucket = True; won_bucket = True
        elif result == "LOSS_HIT":
            moved_bucket = True; won_bucket = False
        elif result == "EXPIRED":
            if abs(exit_price - trade['entry_price']) >= trade['bucket_size']:
                moved_bucket = True
                if pnl > 0: won_bucket = True

        if moved_bucket:
            self.total_significant_moves += 1
            if won_bucket: self.hits_in_direction += 1

        self.active_trades.remove(trade)
        self.closed_trades.append({"pnl": pnl, "result": result})

    def get_stats(self):
        acc = 0.0
        if self.total_significant_moves > 0:
            acc = (self.hits_in_direction / self.total_significant_moves) * 100
        return {
            "accuracy": acc,
            "pnl": self.pnl_cumulative,
            "active": len(self.active_trades),
            "denominator": self.total_significant_moves,
            "closed_count": len(self.closed_trades)
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
            print(f"[*] Found {len(model_files)} files. Downloading...")
            
            loaded = []
            for f in model_files:
                if f.get('download_url'): 
                    self._download_raw(f['download_url'], f['name'], loaded)
            return loaded
        except Exception as e: 
            print(f"Error: {e}")
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
# 4. HISTORICAL DATA & BACKTESTING
# =========================================

async def fetch_historical_candles(session, asset, interval, days):
    """Fetches full Klines (OHLC) for backtesting."""
    # Binance limit is 1000. Calculate how many calls needed.
    # 1m: 1440 * 7 = 10080 candles -> ~11 calls
    end_time = int(time.time() * 1000)
    start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    
    all_candles = []
    
    # We fetch backwards or forwards? Forwards is safer for gaps.
    current_start = start_time
    
    while current_start < end_time:
        url = f"{BINANCE_API_URL}?symbol={asset}&interval={interval}&limit=1000&startTime={current_start}"
        try:
            async with session.get(url) as resp:
                if resp.status != 200: break
                data = await resp.json()
                if not data: break
                
                # Format: [open_time, open, high, low, close, ...]
                # We save: [timestamp, high, low, close]
                batch = [{
                    "ts": int(x[0]),
                    "h": float(x[2]),
                    "l": float(x[3]),
                    "c": float(x[4])
                } for x in data]
                
                all_candles.extend(batch)
                
                # Next start time = last close time + 1ms
                last_ts = data[-1][0]
                current_start = last_ts + 1
                
                if len(batch) < 1000: break # End of data
        except:
            break
            
    return asset, all_candles

async def run_startup_backtest(engines, tracker):
    print(f"\n--- 🔙 STARTING {BACKTEST_DAYS}-DAY BACKTEST ---")
    print(f"Fetching historical data for {len(engines)} assets...")
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_historical_candles(session, e.asset, e.interval, BACKTEST_DAYS) for e in engines]
        results = await asyncio.gather(*tasks)
        
    print("Simulating trades...")
    
    total_candles = 0
    
    for engine in engines:
        # Find the data for this engine
        history = next((r[1] for r in results if r[0] == engine.asset), [])
        if len(history) < 100: continue
        
        # Max sequence length needed for prediction
        max_seq = 20 # Safe buffer
        
        # We assume history is sorted by TS
        for i in range(max_seq, len(history)):
            current_candle = history[i] # The candle that just closed at 'i'
            current_time = datetime.fromtimestamp(current_candle['ts'] / 1000)
            
            # 1. Update Tracker with what happened in THIS candle
            tracker.update(engine.asset, current_candle['c'], current_candle['h'], current_candle['l'], current_time)
            
            # 2. Predict for NEXT candle (using data up to `i` close)
            # Create window of closes ending at i
            window_closes = [c['c'] for c in history[i-max_seq+1 : i+1]]
            
            direction, target, b_size = engine.predict(window_closes)
            
            if direction != 0:
                tracker.add_trade(
                    asset=engine.asset,
                    direction=direction,
                    entry_price=current_candle['c'],
                    bucket_size=b_size,
                    duration_minutes=engine.duration_min,
                    interval=engine.interval,
                    start_time=current_time
                )
                
        total_candles += len(history)

    stats = tracker.get_stats()
    print(f"✅ Backtest Complete ({total_candles} candles processed)")
    print(f"📊 INITIAL STATS: Acc: {stats['accuracy']:.2f}% | PnL: {stats['pnl']:.4f} | Trades: {stats['closed_count']}")
    print("-" * 60)

# =========================================
# 5. LIVE LOOP
# =========================================

async def fetch_live_data(session, engine):
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

async def run_bot_loop(engines, tracker):
    print(f"\n🚀 SWITCHING TO LIVE MODE\n")

    async with aiohttp.ClientSession() as session:
        while True:
            # Sync to next :00 seconds
            now = datetime.now()
            sleep_sec = 60 - now.second - (now.microsecond/1e6) + 0.1
            if sleep_sec < 0: sleep_sec += 60
            
            print(f"⏳ Waiting {sleep_sec:.1f}s...")
            await asyncio.sleep(sleep_sec)

            start_ts = datetime.now()
            print(f"\n--- ⚡ LIVE TICK: {start_ts.strftime('%H:%M:%S')} ---")

            tasks = [fetch_live_data(session, eng) for eng in engines]
            results = await asyncio.gather(*tasks)
            
            signals = []
            
            for engine, closes, high, low in results:
                if not closes: continue
                current_price = closes[-1]
                
                # Update using live candle H/L/C
                tracker.update(engine.asset, current_price, high, low, start_ts)
                
                # Predict
                direction, target, b_size = engine.predict(closes)
                
                if direction != 0:
                    tracker.add_trade(engine.asset, direction, current_price, b_size, engine.duration_min, engine.interval, start_ts)
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
            
            lat = (datetime.now()-start_ts).total_seconds()*1000
            print(f"Latency: {lat:.0f}ms")

# =========================================
# 6. MAIN
# =========================================

def main():
    # 1. Load Models
    loader = ModelLoader(pat=GITHUB_PAT)
    raw = loader.fetch_all_models()
    if not raw: return
    
    engines = [InferenceEngine(m) for m in raw]
    tracker = TradeTracker()
    
    # 2. Run Backtest & Live Loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Run Backtest first
        loop.run_until_complete(run_startup_backtest(engines, tracker))
        # Then switch to Live
        loop.run_until_complete(run_bot_loop(engines, tracker))
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
