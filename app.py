#!/usr/bin/env python3
"""
Kraken Account Observatory
--------------------------
Railway Ready Version
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
logger = logging.getLogger("Observatory")

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
# Railway Configuration:
# 1. Mount your volume to a path (e.g., /data)
# 2. Set Env Var DB_FILE_PATH to /data/observatory.db
DB_FILE = os.getenv('DB_FILE_PATH', 'observatory.db')

FETCH_INTERVAL = 10  # seconds
API_KEY = os.getenv("KRAKEN_KEY", "")
API_SECRET = os.getenv("KRAKEN_SECRET", "")

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
            "User-Agent": "Kraken-Observatory/1.0",
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
        # Ensure directory exists if path is nested
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
    # Ensure directory exists
    db_dir = os.path.dirname(DB_FILE)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''CREATE TABLE IF NOT EXISTS account_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        total_equity REAL,
        total_balance REAL,
        margin_utilized REAL
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS current_state (
        key TEXT PRIMARY KEY,
        data TEXT,
        updated_at REAL
    )''')
    
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_FILE}")

def save_snapshot(equity, balance, margin):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute(
            "INSERT INTO account_history (timestamp, total_equity, total_balance, margin_utilized) VALUES (?, ?, ?, ?)",
            (time.time(), equity, balance, margin)
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
            positions = api.get_open_positions()
            orders = api.get_open_orders()
            tickers = api.get_tickers()

            total_equity = 0.0
            total_balance = 0.0
            margin = 0.0
            
            # Robust account parsing
            if 'accounts' in accounts:
                acc_data = accounts['accounts']
                
                # Determine if it's a list or dict and get an iterator of OBJECTS
                if isinstance(acc_data, dict):
                    acc_iterator = acc_data.values()
                elif isinstance(acc_data, list):
                    acc_iterator = acc_data
                else:
                    logger.warning(f"Unknown accounts structure type: {type(acc_data)}")
                    acc_iterator = []

                for acc in acc_iterator:
                    if not isinstance(acc, dict):
                        # Skip if the item itself is a string/other
                        continue

                    # Safe parsing for balances
                    balances = acc.get('balances', {})
                    if isinstance(balances, dict):
                        if 'usd' in balances:
                            total_balance += float(balances.get('usd', 0))
                        elif 'usdt' in balances:
                            total_balance += float(balances.get('usdt', 0))
                    
                    # Safe parsing for auxiliary
                    aux = acc.get('auxiliary', {})
                    if isinstance(aux, dict):
                        if 'pv' in aux:
                            total_equity += float(aux.get('pv', 0))
                        elif 'equity' in aux:
                            total_equity += float(aux.get('equity', 0))
                            
                        if 'usedMargin' in aux:
                            margin += float(aux.get('usedMargin', 0))

            if total_equity == 0 and total_balance > 0:
                total_equity = total_balance

            save_snapshot(total_equity, total_balance, margin)
            
            update_current_state('positions', positions)
            update_current_state('orders', orders)
            update_current_state('tickers', tickers)
            update_current_state('meta', {
                'last_update': time.time(),
                'equity': total_equity,
                'balance': total_balance,
                'margin': margin
            })
            
            # Count positions safely
            pos_count = 0
            if isinstance(positions, dict):
                p_list = positions.get('openPositions', [])
                if isinstance(p_list, list):
                    pos_count = len(p_list)

            logger.info(f"Updated. Eq: ${total_equity:.2f} | Pos: {pos_count}")

        except Exception as e:
            logger.error(f"Error in fetch loop: {e}", exc_info=True)

        elapsed = time.time() - start_time
        sleep_time = max(0, FETCH_INTERVAL - elapsed)
        time.sleep(sleep_time)

#  ------------------------------------------------------------------
# FLASK WEB APP
# ------------------------------------------------------------------
app = Flask(__name__)

# --- ADD THIS BLOCK HERE ---
# Initialize DB immediately on import so Gunicorn runs it
with app.app_context():
    if not os.path.exists(DB_FILE):
        logger.info("Database file not found. Initializing...")
        init_db()
    else:
        # Run init_db anyway to ensure tables exist (idempotent)
        init_db()
# ---------------------------


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def api_status():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT key, data, updated_at FROM current_state")
        rows = cur.fetchall()
        
        data = {}
        for row in rows:
            try:
                data[row['key']] = json.loads(row['data'])
                data[row['key']]['_updated'] = row['updated_at']
            except:
                data[row['key']] = None
        return jsonify(data)
    except Exception as e:
        logger.error(f"API Status Error: {e}")
        return jsonify({}), 500

@app.route('/api/history')
def api_history():
    try:
        conn = get_db()
        cur = conn.cursor()
        # Limit to last 2880 points (approx 8 hours at 10s intervals) to keep payload light
        cur.execute("SELECT timestamp, total_equity, total_balance, margin_utilized FROM account_history ORDER BY id DESC LIMIT 2880")
        rows = cur.fetchall()
        
        history = []
        for row in reversed(rows):
            history.append({
                'time': row['timestamp'],
                'equity': row['total_equity'],
                'balance': row['total_balance'],
                'margin': row['margin_utilized']
            })
            
        return jsonify(history)
    except Exception as e:
        logger.error(f"API History Error: {e}")
        return jsonify([]), 500

# ------------------------------------------------------------------
# MAIN ENTRY
# ------------------------------------------------------------------
if __name__ == '__main__':
    # Initialize DB
    if not os.path.exists(DB_FILE):
        init_db()
    else:
        init_db() # Ensure schema

    # Start Background Thread
    t = threading.Thread(target=fetch_worker, daemon=True)
    t.start()

    # Get PORT from environment (Railway standard)
    port = int(os.environ.get("PORT", 5000))
    
    # Host must be 0.0.0.0
    print(f"Starting Kraken Observatory on port {port} using DB: {DB_FILE}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)