#!/usr/bin/env python3
"""
Company Primate - Clinical Backend
----------------------------------
Flask + SQLite + Kraken Futures API
Targeting Flex Margin Equity specifically for balance tracking.
Restored full UI routing.

UPDATES:
- Integrated external 'kraken_futures_api.py' library.
- Added 'get_order_events' fetching for the Log page.
"""

import os
import sys
import time
import json
import threading
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from flask import Flask, render_template, jsonify, g

# --- IMPORT EXTERNAL LIBRARY ---
# Assumes the file provided is named 'kraken_futures.py'
try:
    from kraken_futures import KrakenFuturesApi
except ImportError:
    print("CRITICAL: 'kraken_futures.py' not found in directory.")
    sys.exit(1)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Primate")

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
DB_FILE = os.getenv('DB_FILE_PATH', 'primate.db')
FETCH_INTERVAL = 10  # seconds
API_KEY = os.getenv("KRAKEN_KEY", "")
API_SECRET = os.getenv("KRAKEN_SECRET", "")
CONTACT_EMAIL = os.getenv("CONTACT_EMAIL", "contact@companyprimate.com")

if not API_KEY or not API_SECRET:
    logger.warning("WARNING: KRAKEN_KEY or KRAKEN_SECRET env vars are missing.")

# ------------------------------------------------------------------
# DATABASE & STORAGE
# ------------------------------------------------------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db_dir = os.path.dirname(DB_FILE)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
            except OSError as e:
                logger.error(f"Could not create DB directory {db_dir}: {e}")
        
        db = g._database = sqlite3.connect(DB_FILE)
        db.row_factory = sqlite3.Row
    return db

def init_db():
    """Initialize database tables."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS account_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        total_equity REAL,
        total_balance REAL,
        margin_utilized REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS symbol_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        symbol TEXT,
        side TEXT,
        size REAL,
        price REAL,
        mark_price REAL,
        value_usd REAL,
        pnl_usd REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS current_state (
        key TEXT PRIMARY KEY,
        data TEXT,
        updated_at REAL
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

        c.execute(
            "INSERT INTO account_history (timestamp, total_equity, total_balance, margin_utilized) VALUES (?, ?, ?, ?)",
            (now, equity, balance, margin)
        )

        ticker_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers_list if 'symbol' in t}

        for p in positions_list:
            symbol = p.get('symbol')
            if not symbol: continue

            side = p.get('side', 'long')
            size = float(p.get('size', 0))
            entry_price = float(p.get('price', 0))
            mark_price = float(ticker_map.get(symbol, entry_price))
            value_usd = size * mark_price
            pnl = float(p.get('pnl', 0))

            c.execute('''INSERT INTO symbol_history 
                (timestamp, symbol, side, size, price, mark_price, value_usd, pnl_usd) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (now, symbol, side, size, entry_price, mark_price, value_usd, pnl)
            )

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Write Error: {e}")

def update_current_state(key, data):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "INSERT INTO current_state (key, data, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
            (key, json.dumps(data), time.time())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DB Update Error: {e}")

# ------------------------------------------------------------------
# BACKGROUND WORKER
# ------------------------------------------------------------------
def fetch_worker():
    logger.info("Starting background fetcher...")
    # Initialize the imported library class
    api = KrakenFuturesApi(API_KEY, API_SECRET)

    while True:
        try:
            start_time = time.time()
            
            # The external library raises RuntimeError on failure, which is caught below.
            accounts_resp = api.get_accounts()
            positions_resp = api.get_open_positions()
            orders_resp = api.get_open_orders()
            tickers_resp = api.get_tickers()
            
            # --- NEW: Fetch History for Log Page ---
            try:
                events_resp = api.get_order_events()
                history = events_resp.get('elements', [])
            except Exception as hist_e:
                logger.error(f"Failed to fetch history: {hist_e}")
                history = []

            tickers = tickers_resp.get('tickers', [])
            positions = positions_resp.get('openPositions', [])

            # --- CALCULATE PNL MANUALLY ---
            # Kraken REST API does not provide 'pnl' in openPositions, so we compute it.
            # Map symbol -> Mark Price
            mark_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers if 'symbol' in t}

            for p in positions:
                symbol = p.get('symbol')
                if not symbol: continue
                
                # Extract values
                try:
                    entry_price = float(p.get('price', 0))
                    size = float(p.get('size', 0))
                    side = p.get('side', 'long')
                    mark_price = mark_map.get(symbol, entry_price)

                    # Standard Linear PnL Calculation
                    if side == 'long':
                        calculated_pnl = (mark_price - entry_price) * size
                    else:
                        calculated_pnl = (entry_price - mark_price) * size
                    
                    p['pnl'] = calculated_pnl
                except (ValueError, TypeError):
                    p['pnl'] = 0.0
            # -------------------------------

            total_equity = 0.0
            total_balance = 0.0
            margin = 0.0
            
            res = accounts_resp.get('accounts', {})
            
            # CRITICAL FIX: Extracting "Flex Margin Equity"
            if 'flex' in res:
                flex_acc = res['flex']
                total_equity = float(flex_acc.get('marginEquity', 0))
                total_balance = float(flex_acc.get('balance', 0))
                
                aux = flex_acc.get('auxiliary', {})
                # Some API versions put margin in root, some in auxiliary
                margin = float(flex_acc.get('marginUsed', aux.get('usedMargin', 0)))
                logger.info(f"Flex Sync: Equity=${total_equity:.2f}")
            else:
                # Fallback iterate
                for acc_key, acc_val in res.items():
                    if isinstance(acc_val, dict):
                        total_equity += float(acc_val.get('marginEquity', 0))
                        total_balance += float(acc_val.get('balance', 0))
                        margin += float(acc_val.get('marginUsed', acc_val.get('auxiliary', {}).get('usedMargin', 0)))

            save_snapshot(total_equity, total_balance, margin, positions, tickers)
            
            update_current_state('positions', positions)
            update_current_state('orders', orders_resp.get('openOrders', []))
            update_current_state('tickers', tickers)
            update_current_state('history', history) # Save history to DB
            
            update_current_state('meta', {
                'last_update': time.time(),
                'equity': total_equity,
                'balance': total_balance,
                'margin': margin
            })

        except Exception as e:
            # The new library raises exceptions on API errors; we log them here.
            logger.error(f"Error in fetch loop: {e}", exc_info=True)

        elapsed = time.time() - start_time
        time.sleep(max(1, FETCH_INTERVAL - elapsed))

# ------------------------------------------------------------------
# FLASK WEB APP
# ------------------------------------------------------------------
app = Flask(__name__)

with app.app_context():
    init_db()

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    # Restore the frontend
    return render_template('index.html')

@app.route('/api/contact')
def api_contact():
    return jsonify({'email': CONTACT_EMAIL})

@app.route('/api/status')
def api_status():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT key, data, updated_at FROM current_state")
        rows = cur.fetchall()
        data = {}
        for row in rows:
            data[row['key']] = json.loads(row['data'])
            data[row['key']]['_updated'] = row['updated_at']
        return jsonify(data)
    except:
        return jsonify({}), 500

@app.route('/api/balance_history')
def api_balance_history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, total_equity FROM account_history ORDER BY id ASC")
    rows = cur.fetchall()
    return jsonify([{'time': r['timestamp'], 'equity': r['total_equity']} for r in rows])

@app.route('/api/symbols_chart')
def api_symbols_chart():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, symbol, value_usd, pnl_usd FROM symbol_history ORDER BY id ASC")
    rows = cur.fetchall()
    result = {}
    for row in rows:
        sym = row['symbol']
        if sym not in result:
            result[sym] = {'invested': [], 'pnl': []}
        result[sym]['invested'].append({'x': row['timestamp'], 'y': row['value_usd']})
        result[sym]['pnl'].append({'x': row['timestamp'], 'y': row['pnl_usd']})
    return jsonify(result)

@app.route('/api/symbol_detail/<symbol>')
def api_symbol_detail(symbol):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, pnl_usd FROM symbol_history WHERE symbol = ? ORDER BY id ASC", (symbol,))
    pnl_rows = cur.fetchall()
    
    cur.execute("SELECT data FROM current_state WHERE key = 'orders'")
    row = cur.fetchone()
    orders = []
    if row:
        all_orders = json.loads(row['data'])
        orders = [o for o in all_orders if o.get('symbol') == symbol]
        
    return jsonify({
        'pnl_history': [{'x': r['timestamp'], 'y': r['pnl_usd']} for r in pnl_rows],
        'orders': orders
    })

if __name__ == '__main__':
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)