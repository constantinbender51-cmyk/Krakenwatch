#!/usr/bin/env python3
"""
Company Primate - Clinical Backend
----------------------------------
Flask + SQLite + Kraken Futures API
Targeting Flex Margin Equity specifically for balance tracking.
Restored full UI routing.

UPDATES:
- Integrated external 'kraken_futures.py' library.
- Log Page now shows a cumulative history of all open orders ever found.
- Language reverted to English.
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
    
    # Table for account balance history
    c.execute('''CREATE TABLE IF NOT EXISTS account_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        total_equity REAL,
        total_balance REAL,
        margin_utilized REAL
    )''')

    # Table for symbol history (prices/pnl)
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
    
    # NEW: Table for order log (cumulative history of open orders)
    c.execute('''CREATE TABLE IF NOT EXISTS order_log (
        order_id TEXT PRIMARY KEY,
        timestamp REAL,
        symbol TEXT,
        side TEXT,
        size REAL,
        price REAL,
        order_type TEXT,
        raw_data TEXT
    )''')

    # Table for current state (State Store)
    c.execute('''CREATE TABLE IF NOT EXISTS current_state (
        key TEXT PRIMARY KEY,
        data TEXT,
        updated_at REAL
    )''')
    
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_ts ON symbol_history(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_sym ON symbol_history(symbol)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_order_log_ts ON order_log(timestamp)')
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
        logger.error(f"DB Write Error (Snapshot): {e}")

def save_found_orders(orders_list):
    """
    Saves newly found order events (history) to the persistent log.
    Uses INSERT OR IGNORE to avoid duplicates based on order_id.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        count = 0
        for o in orders_list:
            # Kraken Futures Order ID
            oid = o.get('orderId')
            if not oid: continue
            
            # Use Kraken's timestamp for historical accuracy, convert from ms to s
            order_time_ms = o.get('timestamp')
            # Fallback to current time if timestamp is missing or invalid
            order_timestamp = float(order_time_ms) / 1000 if isinstance(order_time_ms, (int, float)) else time.time()
            
            symbol = o.get('tradeable') # Use 'tradeable' for symbol in history API
            side = o.get('direction', 'buy') # Use 'direction' for side in history API
            size = float(o.get('quantity', 0)) # Use 'quantity' for size in history API
            # Price for history items can be 'limitPrice', 'stopPrice', 'price', or 'fillPrice'
            # Prioritize limitPrice, then fillPrice, default to 0 if not found.
            price = float(o.get('limitPrice', 0) or o.get('fillPrice', 0))
            otype = o.get('orderType', 'limit') # e.g., 'Limit', 'Market', 'Stop', 'TakeProfit'

            c.execute('''INSERT OR IGNORE INTO order_log 
                (order_id, timestamp, symbol, side, size, price, order_type, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                (oid, order_timestamp, symbol, side, size, price, otype, json.dumps(o)))
            
            if c.rowcount > 0:
                count += 1
                
        conn.commit()
        conn.close()
        if count > 0:
            logger.info(f"New order events archived: {count}")
            
    except Exception as e:
        logger.error(f"DB Write Error (Order Log): {e}")

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
    api = KrakenFuturesApi(API_KEY, API_SECRET)

    while True:
        try:
            start_time = time.time()
            
            accounts_resp = api.get_accounts()
            positions_resp = api.get_open_positions()
            orders_resp = api.get_open_orders()
            tickers_resp = api.get_tickers()
            
            # --- ORDER HISTORY LOGIC ---
            # Capture currently open orders and persist them
            open_orders = orders_resp.get('openOrders', [])
            save_found_orders(open_orders)
            # ---------------------------

            tickers = tickers_resp.get('tickers', [])
            positions = positions_resp.get('openPositions', [])

            # --- PNL CALCULATION ---
            mark_map = {t['symbol']: float(t.get('markPrice', 0)) for t in tickers if 'symbol' in t}

            for p in positions:
                symbol = p.get('symbol')
                if not symbol: continue
                
                try:
                    entry_price = float(p.get('price', 0))
                    size = float(p.get('size', 0))
                    side = p.get('side', 'long')
                    mark_price = mark_map.get(symbol, entry_price)

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
            
            if 'flex' in res:
                flex_acc = res['flex']
                total_equity = float(flex_acc.get('marginEquity', 0))
                total_balance = float(flex_acc.get('balance', 0))
                
                aux = flex_acc.get('auxiliary', {})
                margin = float(flex_acc.get('marginUsed', aux.get('usedMargin', 0)))
                logger.info(f"Flex Sync: Equity=${total_equity:.2f}")
            else:
                for acc_key, acc_val in res.items():
                    if isinstance(acc_val, dict):
                        total_equity += float(acc_val.get('marginEquity', 0))
                        total_balance += float(acc_val.get('balance', 0))
                        margin += float(acc_val.get('marginUsed', acc_val.get('auxiliary', {}).get('usedMargin', 0)))

            save_snapshot(total_equity, total_balance, margin, positions, tickers)
            
            update_current_state('positions', positions)
            update_current_state('orders', open_orders)
            update_current_state('tickers', tickers)
            
            update_current_state('meta', {
                'last_update': time.time(),
                'equity': total_equity,
                'balance': total_balance,
                'margin': margin
            })

        except Exception as e:
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
    return render_template('index.html')

@app.route('/api/contact')
def api_contact():
    return jsonify({'email': CONTACT_EMAIL})

@app.route('/api/status')
def api_status():
    """
    Returns status for the Log page.
    Now retrieves real history from the 'order_log' database table.
    """
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Get the last 100 orders from the persistent history
        cur.execute("SELECT * FROM order_log ORDER BY timestamp DESC LIMIT 100")
        rows = cur.fetchall()
        
        history = []
        for row in rows:
            history.append({
                'order_id': row['order_id'],
                'timestamp': row['timestamp'],
                'symbol': row['symbol'],
                'side': row['side'],
                'size': row['size'],
                'price': row['price'],
                'type': row['order_type']
            })
            
        # Optional: Get Meta data
        cur.execute("SELECT data FROM current_state WHERE key = 'meta'")
        meta_row = cur.fetchone()
        meta = json.loads(meta_row['data']) if meta_row else {}

        return jsonify({
            'history': history,
            'meta': meta
        })
    except Exception as e:
        logger.error(f"API Status Error: {e}")
        return jsonify({'history': [], 'error': str(e)}), 500

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
    
    # Active orders for the symbol page
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