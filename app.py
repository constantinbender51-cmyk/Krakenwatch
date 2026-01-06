#!/usr/bin/env python3
"""
Company Primate - Professional Web Presence
-------------------------------------------
Backend: Flask + SQLite + Kraken Futures API
"""

import os
import sys
import time
import json
import threading
import sqlite3
import logging
import base64
import hashlib
import hmac
import urllib.parse
from datetime import datetime
from typing import Dict, Any, Optional

import requests
from flask import Flask, render_template, jsonify, g, request

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
# API CLIENT
# ------------------------------------------------------------------
class KrakenFuturesApi:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://futures.kraken.com") -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self._nonce_counter = 0

    def _create_nonce(self) -> str:
        if self._nonce_counter > 9_999:
            self._nonce_counter = 0
        counter_str = f"{self._nonce_counter:05d}"
        self._nonce_counter += 1
        return f"{int(time.time() * 1_000)}{counter_str}"

    def _sign_request(self, endpoint: str, nonce: str, post_data: str = "") -> str:
        path = endpoint[12:] if endpoint.startswith("/derivatives") else endpoint
        message = (post_data + nonce + path).encode()
        sha256_hash = hashlib.sha256(message).digest()
        secret_decoded = base64.b64decode(self.api_secret)
        sig = hmac.new(secret_decoded, sha256_hash, hashlib.sha512).digest()
        return base64.b64encode(sig).decode()

    def _request(self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params or {}
        url = self.base_url + endpoint
        nonce = self._create_nonce()
        post_data = ""
        headers = {
            "APIKey": self.api_key,
            "Nonce": nonce,
            "User-Agent": "Primate-Observatory/2.0",
        }

        if method.upper() == "POST":
            post_data = urllib.parse.urlencode(params)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif params:
            url += "?" + urllib.parse.urlencode(params)

        headers["Authent"] = self._sign_request(endpoint, nonce, post_data)

        try:
            rsp = requests.request(method, url, headers=headers, data=post_data or None, timeout=5)
            if not rsp.ok:
                logger.error(f"API Error {rsp.status_code}: {rsp.text}")
                return {} 
            return rsp.json()
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return {}

    def get_accounts(self) -> Dict[str, Any]:
        return self._request("GET", "/derivatives/api/v3/accounts")

    def get_open_positions(self) -> Dict[str, Any]:
        return self._request("GET", "/derivatives/api/v3/openpositions")

    def get_open_orders(self) -> Dict[str, Any]:
        return self._request("GET", "/derivatives/api/v3/openorders")
    
    def get_tickers(self) -> Dict[str, Any]:
        return self._request("GET", "/derivatives/api/v3/tickers")

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
    """Initialize database tables if they don't exist."""
    db_dir = os.path.dirname(DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # 1. Account History (Global Equity)
    c.execute('''CREATE TABLE IF NOT EXISTS account_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        total_equity REAL,
        total_balance REAL,
        margin_utilized REAL
    )''')

    # 2. Symbol History (Per-symbol tracking)
    # Value = size * price, PnL = current PnL
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

    # 3. Current State (Latest API Dump)
    c.execute('''CREATE TABLE IF NOT EXISTS current_state (
        key TEXT PRIMARY KEY,
        data TEXT,
        updated_at REAL
    )''')
    
    # Indexes for speed
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_ts ON symbol_history(timestamp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sym_hist_sym ON symbol_history(symbol)')

    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_FILE}")

def save_snapshot(equity, balance, margin, positions_list, tickers_list):
    """Save global and per-symbol snapshots"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        now = time.time()

        # 1. Global
        c.execute(
            "INSERT INTO account_history (timestamp, total_equity, total_balance, margin_utilized) VALUES (?, ?, ?, ?)",
            (now, equity, balance, margin)
        )

        # 2. Per Symbol
        # Create a map of tickers for fast price lookup
        ticker_map = {t['symbol']: t.get('markPrice', 0) for t in tickers_list}

        for p in positions_list:
            symbol = p.get('symbol')
            if not symbol: continue

            side = p.get('side', 'long')
            size = float(p.get('size', 0))
            entry_price = float(p.get('price', 0))
            mark_price = float(ticker_map.get(symbol, entry_price)) # Fallback to entry if no ticker
            
            # Calculate Value in USD (Size * Mark Price)
            value_usd = size * mark_price
            
            # PnL (API usually provides it, but we can calc approx if missing)
            pnl = float(p.get('pnl', 0))
            if pnl == 0 and size > 0:
                if side == 'long':
                    pnl = (mark_price - entry_price) * size
                else:
                    pnl = (entry_price - mark_price) * size

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
    api = KrakenFuturesApi(API_KEY, API_SECRET)

    while True:
        try:
            start_time = time.time()
            
            accounts = api.get_accounts()
            positions_resp = api.get_open_positions()
            orders_resp = api.get_open_orders()
            tickers_resp = api.get_tickers()

            # Parse Tickers
            tickers = []
            if isinstance(tickers_resp, dict) and 'tickers' in tickers_resp:
                tickers = tickers_resp['tickers']

            # Parse Positions
            positions = []
            if isinstance(positions_resp, dict) and 'openPositions' in positions_resp:
                positions = positions_resp['openPositions']

            # Calculate Global Metrics
            total_equity = 0.0
            total_balance = 0.0
            margin = 0.0
            
            # Account parsing logic
            payload = accounts.get('result', accounts) if isinstance(accounts, dict) else accounts
            acc_data = payload.get('accounts', payload) if isinstance(payload, dict) else payload

            # Normalize acc_data to list
            acc_list = []
            if isinstance(acc_data, dict):
                acc_list = acc_data.values()
            elif isinstance(acc_data, list):
                acc_list = acc_data

            for acc in acc_list:
                if not isinstance(acc, dict): continue
                
                # Balance
                bals = acc.get('balances', {})
                if 'usd' in bals: total_balance += float(bals['usd'])
                elif 'usdt' in bals: total_balance += float(bals['usdt'])
                
                # Equity
                aux = acc.get('auxiliary', {})
                val = float(acc.get('marginEquity', aux.get('marginEquity', aux.get('pv', aux.get('equity', 0)))))
                total_equity += val
                
                # Margin
                margin += float(aux.get('usedMargin', 0))

            if total_equity == 0 and total_balance > 0:
                total_equity = total_balance

            # Save Data
            save_snapshot(total_equity, total_balance, margin, positions, tickers)
            
            update_current_state('positions', positions)
            update_current_state('orders', orders_resp.get('openOrders', []))
            update_current_state('tickers', tickers)
            update_current_state('meta', {
                'last_update': time.time(),
                'equity': total_equity,
                'balance': total_balance,
                'margin': margin
            })

            logger.info(f"Updated. Eq: ${total_equity:.2f} | Pos: {len(positions)}")

        except Exception as e:
            logger.error(f"Error in fetch loop: {e}", exc_info=True)

        elapsed = time.time() - start_time
        sleep_time = max(0, FETCH_INTERVAL - elapsed)
        time.sleep(sleep_time)

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

# --- APIs ---

@app.route('/api/contact')
def api_contact():
    return jsonify({'email': CONTACT_EMAIL})

@app.route('/api/status')
def api_status():
    """Get current snapshot of everything"""
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
    """Get global equity history for the Balance tab"""
    conn = get_db()
    cur = conn.cursor()
    # Limit to ~24h (8640 points at 10s is too much, let's limit to 2000)
    cur.execute("SELECT timestamp, total_equity FROM account_history ORDER BY id DESC LIMIT 2000")
    rows = cur.fetchall()
    return jsonify([{'time': r['timestamp'], 'equity': r['total_equity']} for r in reversed(rows)])

@app.route('/api/symbols_chart')
def api_symbols_chart():
    """
    Get aggregated history for the Main Page Chart.
    Returns: { 'symbol_A': [{time, value_usd}, ...], 'symbol_B': ... }
    """
    conn = get_db()
    cur = conn.cursor()
    # Get last 2000 entries of symbol history
    # Note: simple query, client side will handle the multi-line logic
    cur.execute("SELECT timestamp, symbol, value_usd FROM symbol_history ORDER BY id DESC LIMIT 5000")
    rows = cur.fetchall()
    
    result = {}
    # Organize by symbol
    for row in reversed(rows):
        sym = row['symbol']
        if sym not in result:
            result[sym] = []
        result[sym].append({'x': row['timestamp'], 'y': row['value_usd']})
    
    return jsonify(result)

@app.route('/api/symbol_detail/<symbol>')
def api_symbol_detail(symbol):
    """Get PnL history and orders for a specific symbol"""
    conn = get_db()
    cur = conn.cursor()
    
    # 1. PnL History
    cur.execute(
        "SELECT timestamp, pnl_usd FROM symbol_history WHERE symbol = ? ORDER BY id DESC LIMIT 1000", 
        (symbol,)
    )
    pnl_rows = cur.fetchall()
    pnl_history = [{'x': r['timestamp'], 'y': r['pnl_usd']} for r in reversed(pnl_rows)]
    
    # 2. Orders (Filter from current open orders)
    # Note: We only have *open* orders in current_state. 
    # If we wanted historical orders, we'd need to fetch fill history from API, 
    # but for now we filter the cached open orders.
    cur.execute("SELECT data FROM current_state WHERE key = 'orders'")
    row = cur.fetchone()
    orders = []
    if row:
        all_orders = json.loads(row['data'])
        orders = [o for o in all_orders if o.get('symbol') == symbol]
        
    return jsonify({
        'pnl_history': pnl_history,
        'orders': orders
    })

# ------------------------------------------------------------------
# MAIN ENTRY
# ------------------------------------------------------------------
if __name__ == '__main__':
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    print(f"Company Primate Online on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)