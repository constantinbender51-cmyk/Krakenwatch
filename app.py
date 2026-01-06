#!/usr/bin/env python3
"""
Company Primate - Clinical Backend
----------------------------------
Flask + SQLite + Kraken Futures API
Targeting Flex Margin Equity specifically for balance tracking.
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
from flask import Flask, render_template, jsonify, g

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
            "User-Agent": "Primate-Observatory/3.0",
        }

        if method.upper() == "POST":
            post_data = urllib.parse.urlencode(params)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif params:
            url += "?" + urllib.parse.urlencode(params)

        headers["Authent"] = self._sign_request(endpoint, nonce, post_data)

        try:
            rsp = requests.request(method, url, headers=headers, data=post_data or None, timeout=8)
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
    api = KrakenFuturesApi(API_KEY, API_SECRET)

    while True:
        try:
            start_time = time.time()
            
            accounts_resp = api.get_accounts()
            positions_resp = api.get_open_positions()
            orders_resp = api.get_open_orders()
            tickers_resp = api.get_tickers()

            tickers = tickers_resp.get('tickers', [])
            positions = positions_resp.get('openPositions', [])

            # Extraction logic for Flex Margin Equity
            total_equity = 0.0
            total_balance = 0.0
            margin = 0.0
            
            # The API returns results nested under 'accounts'
            res = accounts_resp.get('accounts', {})
            
            # Specifically check for the 'flex' account key
            if 'flex' in res:
                flex_acc = res['flex']
                total_equity = float(flex_acc.get('marginEquity', 0))
                total_balance = float(flex_acc.get('balance', 0))
                
                # Try to get utilized margin from auxiliary if not in root
                aux = flex_acc.get('auxiliary', {})
                margin = float(flex_acc.get('marginUsed', aux.get('usedMargin', 0)))
                
                logger.info(f"Flex Account Found: Equity=${total_equity}")
            else:
                # Fallback: Iterate through all accounts if 'flex' isn't the key
                # This handles cases where account keys might be named differently (e.g. fi_xbt_usd)
                for acc_key, acc_val in res.items():
                    if isinstance(acc_val, dict):
                        # Add up equities from all margin accounts
                        eq = float(acc_val.get('marginEquity', 0))
                        total_equity += eq
                        total_balance += float(acc_val.get('balance', 0))
                        margin += float(acc_val.get('marginUsed', acc_val.get('auxiliary', {}).get('usedMargin', 0)))

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

            logger.info(f"Sync complete. Flex Eq: ${total_equity:.2f}")

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
    return "Clinical Backend Running"

@app.route('/api/status')
def api_status():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT key, data, updated_at FROM current_state")
        rows = cur.fetchall()
        data = {row['key']: json.loads(row['data']) for row in rows}
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/balance_history')
def api_balance_history():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT timestamp, total_equity FROM account_history ORDER BY timestamp ASC")
    rows = cur.fetchall()
    return jsonify([{'time': r['timestamp'], 'equity': r['total_equity']} for r in rows])

if __name__ == '__main__':
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)