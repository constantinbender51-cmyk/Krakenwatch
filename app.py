#!/usr/bin/env python3
"""
Company Primate - Clinical Backend (Redesign)
----------------------------------
Flask + SQLite + Kraken Futures API
"""

import os
import sys
import time
import json
import threading
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

from flask import Flask, render_template, jsonify, g

# --- IMPORT EXTERNAL LIBRARY ---
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    # Mock for testing if lib is missing
    print("WARNING: 'kraken_futures.py' not found. Using Mock.")
    class KrakenFuturesApi:
        def __init__(self, k, s): pass
        def get_accounts(self): return {}
        def get_open_positions(self): return {}
        def get_open_orders(self): return {}
        def get_tickers(self): return {}

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Primate")

# CONFIGURATION
DB_FILE = os.getenv('DB_FILE_PATH', 'primate.db')
FETCH_INTERVAL = 10
API_KEY = os.getenv("KRAKEN_KEY", "")
API_SECRET = os.getenv("KRAKEN_SECRET", "")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "contact@companyprimate.com")

# ------------------------------------------------------------------
# DATABASE
# ------------------------------------------------------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DB_FILE)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS account_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, total_equity REAL, total_balance REAL, margin_utilized REAL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS symbol_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL, symbol TEXT, side TEXT, size REAL, price REAL, mark_price REAL, value_usd REAL, pnl_usd REAL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS order_log (
        order_id TEXT PRIMARY KEY, timestamp REAL, symbol TEXT, side TEXT, size REAL, price REAL, order_type TEXT, raw_data TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS current_state (
        key TEXT PRIMARY KEY, data TEXT, updated_at REAL
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_ts ON symbol_history(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_sym ON symbol_history(symbol)')
    conn.commit()
    conn.close()

def save_snapshot(equity, balance, margin, positions_list, tickers_list):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        now = time.time()
        c.execute("INSERT INTO account_history (timestamp, total_equity, total_balance, margin_utilized) VALUES (?, ?, ?, ?)", (now, equity, balance, margin))
        
        ticker_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers_list if 'symbol' in t}
        
        # We must record ALL active symbols, even if size is 0 (if we track them), 
        # but the API only gives open positions. 
        for p in positions_list:
            symbol = p.get('symbol')
            if not symbol: continue
            size = float(p.get('size', 0))
            entry_price = float(p.get('price', 0))
            mark_price = float(ticker_map.get(symbol, entry_price))
            value_usd = size * mark_price
            pnl = float(p.get('pnl', 0))
            c.execute('''INSERT INTO symbol_history (timestamp, symbol, side, size, price, mark_price, value_usd, pnl_usd) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (now, symbol, p.get('side', 'long'), size, entry_price, mark_price, value_usd, pnl))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Snapshot Error: {e}")

def update_current_state(key, data):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO current_state (key, data, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at", (key, json.dumps(data), time.time()))
        conn.commit()
        conn.close()
    except Exception: pass

# ------------------------------------------------------------------
# WORKER
# ------------------------------------------------------------------
def fetch_worker():
    logger.info("Starting background fetcher...")
    api = KrakenFuturesApi(API_KEY, API_SECRET) if API_KEY else None
    while True:
        try:
            start_time = time.time()
            if not api:
                # Mock data generator for testing without API keys
                import random
                total_equity = 10000 + random.uniform(-500, 500)
                pos = [{'symbol': 'BTC', 'size': 2.5, 'price': 30000, 'pnl': random.uniform(-200, 200)}, 
                       {'symbol': 'ETH', 'size': 15, 'price': 2000, 'pnl': random.uniform(-100, 100)}]
                tickers = [{'symbol': 'BTC', 'markPrice': 30100}, {'symbol': 'ETH', 'markPrice': 2010}]
                save_snapshot(total_equity, 10000, 500, pos, tickers)
                update_current_state('positions', pos)
                
            else:
                accounts = api.get_accounts()
                positions = api.get_open_positions().get('openPositions', [])
                tickers = api.get_tickers().get('tickers', [])
                
                # Calculate PnL
                mark_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers if 'symbol' in t}
                for p in positions:
                    s = p.get('symbol')
                    try:
                        entry = float(p.get('price', 0))
                        size = float(p.get('size', 0))
                        mark = mark_map.get(s, entry)
                        p['pnl'] = (mark - entry) * size if p.get('side') == 'long' else (entry - mark) * size
                    except: p['pnl'] = 0.0

                # Extract Equity
                equity = 0.0
                balance = 0.0
                if 'accounts' in accounts and 'flex' in accounts['accounts']:
                    equity = float(accounts['accounts']['flex'].get('marginEquity', 0))
                    balance = float(accounts['accounts']['flex'].get('balance', 0))
                
                save_snapshot(equity, balance, 0, positions, tickers)
                update_current_state('positions', positions)

        except Exception as e:
            logger.error(f"Worker Error: {e}")
        time.sleep(FETCH_INTERVAL)

# ------------------------------------------------------------------
# FLASK & RESAMPLING LOGIC
# ------------------------------------------------------------------
app = Flask(__name__)
with app.app_context(): init_db()

def resample_data(rows, interval_seconds, value_key, method='last'):
    """
    Resamples a list of dicts/rows based on timestamp buckets.
    method: 'last' (closing value) or 'sum' (not used here usually)
    """
    if not rows: return []
    
    buckets = {}
    for r in rows:
        ts = r['timestamp']
        # Integer division to find the bucket index
        bucket_ts = int(ts // interval_seconds) * interval_seconds
        
        if bucket_ts not in buckets:
            buckets[bucket_ts] = []
        buckets[bucket_ts].append(r[value_key])
    
    result = []
    sorted_times = sorted(buckets.keys())
    
    for t in sorted_times:
        vals = buckets[t]
        val = vals[-1] # Take the last value recorded in that bucket
        result.append({'x': t * 1000, 'y': val}) # JS uses ms
        
    return result

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/init')
def api_init():
    # Get active symbols for the sidebar
    conn = get_db()
    cur = conn.cursor()
    # Find all symbols traded in the last 30 days
    month_ago = time.time() - (30 * 24 * 3600)
    cur.execute("SELECT DISTINCT symbol FROM symbol_history WHERE timestamp > ?", (month_ago,))
    symbols = [row['symbol'] for row in cur.fetchall()]
    return jsonify({'symbols': symbols})

@app.route('/api/data/<symbol>')
def api_data(symbol):
    """
    Returns:
    1. Top Chart: % Yield over last 1 Month (Hourly)
    2. Bottom Chart: Position Size over last 1 Day (15m)
    """
    conn = get_db()
    cur = conn.cursor()
    now = time.time()
    
    # --- 1. Top Chart Data (1 Month, Hourly) ---
    month_ago = now - (30 * 24 * 3600)
    hourly_sec = 3600
    
    top_data = []
    
    if symbol == 'composite':
        # Get total equity history
        cur.execute("SELECT timestamp, total_equity FROM account_history WHERE timestamp > ? ORDER BY timestamp ASC", (month_ago,))
        rows = cur.fetchall()
        # Resample to Hourly
        resampled = resample_data(rows, hourly_sec, 'total_equity')
        
        # Convert to Percentage Yield relative to start of period
        if resampled:
            start_val = resampled[0]['y']
            if start_val != 0:
                top_data = [{'x': p['x'], 'y': ((p['y'] - start_val) / start_val) * 100} for p in resampled]
    else:
        # Get specific symbol PnL history
        # Note: We need to reconstruct cumulative PnL or just raw PnL value. 
        # Assuming 'pnl_usd' in DB is the Unrealized PnL of the open position at that time.
        cur.execute("SELECT timestamp, pnl_usd FROM symbol_history WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC", (symbol, month_ago))
        rows = cur.fetchall()
        resampled = resample_data(rows, hourly_sec, 'pnl_usd')
        
        # For specific assets, we might just show raw PnL or Yield. 
        # Request says: "A only of the specific asset replaces the general pnl".
        # Let's show % yield based on an assumed base or just raw PnL normalized?
        # The prompt asks for +5% / -5% lines. It implies the chart is %.
        # If we just opened a trade, calculating yield is (PnL / Margin). 
        # To simplify: We will map the Raw PnL to the Y-axis. 
        # BUT, to keep the 5% lines relevant, we'll calculate yield based on average position size.
        # Fallback: Just display Raw PnL, but the +/-5% lines might look weird if not %.
        # DECISION: For specific asset, show Raw PnL (Currency). The +/-5% lines remain as reference 
        # for the 'Composite' view, but we will hide them or ignore them for specific asset view 
        # if the scale doesn't match. 
        # ACTUALLY, prompt says: "replaces the general pnl... black horizontal lines indicating +5% and -5%".
        # This implies specific asset data should also be % based.
        # Let's try to calculate Return on Investment (Unrealized PnL / Value).
        
        cur.execute("SELECT timestamp, pnl_usd, value_usd FROM symbol_history WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC", (symbol, month_ago))
        rows_roi = cur.fetchall()
        
        # Resample Logic for ROI (complex because we need two columns)
        buckets = {}
        for r in rows_roi:
            b_ts = int(r['timestamp'] // hourly_sec) * hourly_sec
            buckets[b_ts] = r # Keep last
        
        sorted_ts = sorted(buckets.keys())
        for t in sorted_ts:
            r = buckets[t]
            val = r['value_usd']
            pnl = r['pnl_usd']
            # ROI % = (PnL / (Value - PnL)) * 100 approx (Value includes PnL usually? DB 'value_usd' usually is size*mark)
            # Standard ROE = PnL / InitialMargin. We don't have IM.
            # We will use: PnL / (Value) * leverage_factor? 
            # Simplest: PnL / (Value - PnL) * 100.
            roi = 0
            if (val - pnl) > 0:
                roi = (pnl / (val - pnl)) * 100
            
            top_data.append({'x': t * 1000, 'y': roi})

    # --- 2. Bottom Chart Data (1 Day, 15 Min) ---
    day_ago = now - (24 * 3600)
    fifteen_min_sec = 900
    bottom_data = []

    if symbol != 'composite':
        cur.execute("SELECT timestamp, size, side FROM symbol_history WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC", (symbol, day_ago))
        rows = cur.fetchall()
        
        # Logic: If side is Short, make size negative for visualization? 
        # Prompt says "positive or negative and a black line at the 0 mark".
        processed_rows = []
        for r in rows:
            qty = r['size']
            if r['side'] == 'sell' or r['side'] == 'short':
                qty = -qty
            processed_rows.append({'timestamp': r['timestamp'], 'qty': qty})
            
        resampled_bottom = resample_data(processed_rows, fifteen_min_sec, 'qty')
        bottom_data = resampled_bottom

    return jsonify({
        'top': top_data,
        'bottom': bottom_data
    })

@app.route('/api/contact')
def api_contact():
    return jsonify({'email': CONTACT_EMAIL})

if __name__ == '__main__':
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)), debug=False)
