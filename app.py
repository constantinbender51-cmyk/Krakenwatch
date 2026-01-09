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

from flask import Flask, render_template, jsonify, g
from dotenv import load_dotenv  # <--- NEW: Import dotenv

# <--- NEW: Load environment variables from .env file immediately
load_dotenv()

try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("WARNING: 'kraken_futures.py' not found. Using Mock.")
    class KrakenFuturesApi:
        def __init__(self, k, s): pass
        def get_accounts(self): return {}
        def get_open_positions(self): return {}
        def get_open_orders(self): return {}
        def get_tickers(self): return {}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Primate")

DB_FILE = os.getenv('DB_FILE_PATH', 'primate.db')
FETCH_INTERVAL = 10
API_KEY = os.getenv("K_API_KEY", "")
API_SECRET = os.getenv("K_API_SECRET", "")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "y.z.com")

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

def fetch_worker():
    logger.info("Starting background fetcher...")
    api = KrakenFuturesApi(API_KEY, API_SECRET) if API_KEY else None
    while True:
        try:
            start_time = time.time()
            if not api:
                # Mock Data
                import random
                eq = 10000 + random.uniform(-100, 100)
                pos = [{'symbol': 'PF_BTCUSD', 'size': 1.5, 'price': 30000, 'pnl': random.uniform(-50,50)}]
                save_snapshot(eq, 10000, 0, pos, [{'symbol': 'PF_BTCUSD', 'markPrice': 30050}])
            else:
                accounts = api.get_accounts()
                positions = api.get_open_positions().get('openPositions', [])
                tickers = api.get_tickers().get('tickers', [])
                
                mark_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers if 'symbol' in t}
                for p in positions:
                    s = p.get('symbol')
                    try:
                        entry = float(p.get('price', 0))
                        size = float(p.get('size', 0))
                        mark = mark_map.get(s, entry)
                        p['pnl'] = (mark - entry) * size if p.get('side') == 'long' else (entry - mark) * size
                    except: p['pnl'] = 0.0

                equity = 0.0
                if 'accounts' in accounts and 'flex' in accounts['accounts']:
                    equity = float(accounts['accounts']['flex'].get('marginEquity', 0))
                
                save_snapshot(equity, 0, 0, positions, tickers)
        except Exception as e:
            logger.error(f"Worker Error: {e}")
        time.sleep(FETCH_INTERVAL)

app = Flask(__name__)
with app.app_context(): init_db()

def resample_data(rows, interval_seconds, value_key):
    if not rows: return []
    buckets = {}
    for r in rows:
        bucket_ts = int(r['timestamp'] // interval_seconds) * interval_seconds
        buckets[bucket_ts] = r[value_key]
    return [{'x': t * 1000, 'y': buckets[t]} for t in sorted(buckets.keys())]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/init')
def api_init():
    conn = get_db()
    cur = conn.cursor()
    month_ago = time.time() - (30 * 24 * 3600)
    cur.execute("SELECT DISTINCT symbol FROM symbol_history WHERE timestamp > ?", (month_ago,))
    symbols = [row['symbol'] for row in cur.fetchall()]
    return jsonify({'symbols': symbols})

@app.route('/api/data/<symbol>')
def api_data(symbol):
    conn = get_db()
    cur = conn.cursor()
    now = time.time()
    
    # Top Chart (1 Month, Hourly)
    month_ago = now - (30 * 24 * 3600)
    top_data = []
    
    if symbol == 'composite':
        cur.execute("SELECT timestamp, total_equity FROM account_history WHERE timestamp > ? ORDER BY timestamp ASC", (month_ago,))
        resampled = resample_data(cur.fetchall(), 3600, 'total_equity')
        if resampled:
            start_val = resampled[0]['y']
            if start_val != 0:
                top_data = [{'x': p['x'], 'y': ((p['y'] - start_val) / start_val) * 100} for p in resampled]
    else:
        cur.execute("SELECT timestamp, pnl_usd, value_usd FROM symbol_history WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC", (symbol, month_ago))
        rows = cur.fetchall()
        buckets = {}
        for r in rows:
            ts = int(r['timestamp'] // 3600) * 3600
            val = r['value_usd']
            pnl = r['pnl_usd']
            roi = 0
            if (val - pnl) > 0: roi = (pnl / (val - pnl)) * 100
            buckets[ts] = roi
        top_data = [{'x': t*1000, 'y': buckets[t]} for t in sorted(buckets.keys())]

    # Bottom Chart (1 Day, 15m)
    bottom_data = []
    if symbol != 'composite':
        day_ago = now - (24 * 3600)
        cur.execute("SELECT timestamp, size, side FROM symbol_history WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC", (symbol, day_ago))
        rows = cur.fetchall()
        
        buckets = {}
        for r in rows:
            ts = int(r['timestamp'] // 900) * 900
            qty = r['size']
            if r['side'] in ['sell', 'short']: qty = -qty
            buckets[ts] = qty
        bottom_data = [{'timestamp': t, 'qty': buckets[t]} for t in sorted(buckets.keys())]

    return jsonify({'top': top_data, 'bottom': bottom_data})

@app.route('/api/contact')
def api_contact():
    return jsonify({'email': CONTACT_EMAIL})

if __name__ == '__main__':
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)), debug=False)
