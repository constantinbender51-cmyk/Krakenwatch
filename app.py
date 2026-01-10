import os
import time
import base64
import requests
from datetime import datetime, timedelta
from kraken_futures import KrakenFuturesApi

# --- Configuration ---
GITHUB_REPO = "constantinbender51-cmyk/Models"
GITHUB_FILE_PATH = "kraken_logs.txt"
GITHUB_TOKEN = os.getenv("PAT")

KRAKEN_KEY = os.getenv("KRAKEN_FUTURES_KEY")
KRAKEN_SECRET = os.getenv("KRAKEN_FUTURES_SECRET")

# --- GitHub Helper ---
def append_to_github(lines_to_add):
    if not lines_to_add:
        return

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    try:
        r = requests.get(url, headers=headers)
        sha = None
        existing_content = ""

        if r.status_code == 200:
            data = r.json()
            sha = data.get("sha")
            existing_content = base64.b64decode(data.get("content", "")).decode("utf-8")
        elif r.status_code == 404:
            print(f"File {GITHUB_FILE_PATH} not found, creating new one.")
        else:
            print(f"Error fetching file: {r.status_code}")
            return

        if existing_content and not existing_content.endswith("\n"):
            existing_content += "\n"
        
        new_chunk = "\n".join(lines_to_add)
        full_content = existing_content + new_chunk
        encoded_content = base64.b64encode(full_content.encode("utf-8")).decode("utf-8")
        
        payload = {
            "message": f"Update positions {datetime.now().strftime('%H:%M:%S')}",
            "content": encoded_content
        }
        if sha:
            payload["sha"] = sha

        put_resp = requests.put(url, headers=headers, json=payload)
        if put_resp.status_code in [200, 201]:
            print(f"Logged {len(lines_to_add)} line(s) to GitHub.")
        else:
            print(f"Failed to update GitHub: {put_resp.status_code}")
            
    except Exception as e:
        print(f"GitHub Error: {e}")

# --- Kraken Helper ---
def get_position_details(api_client):
    """
    Returns a dict: { symbol: {'size': float, 'entryPrice': float} }
    """
    try:
        response = api_client.get_open_positions()
        positions = response.get("openPositions", [])
        
        pos_map = {}
        for p in positions:
            symbol = p.get("symbol")
            raw_size = float(p.get("size", 0.0))
            side = p.get("side", "long")
            # Entry price is usually returned as 'price'
            entry_price = float(p.get("price", 0.0))
            
            # Apply sign to size
            if side == "short":
                size = -1 * abs(raw_size)
            else:
                size = abs(raw_size)
            
            pos_map[symbol] = {
                "size": size,
                "entryPrice": entry_price
            }
        return pos_map
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return {}

def get_recent_fill_price(api_client, symbol, lookback_seconds=60):
    """
    Fetches the most recent fill price for a symbol.
    Returns (price, timestamp_dt) or (None, None).
    """
    try:
        # Note: Based on your logs, the key is 'elements', not 'fills'
        response = api_client.get_fills() 
        fills = response.get("elements", []) # Changed from 'fills' to 'elements'
        
        cutoff_time = datetime.now() - timedelta(seconds=lookback_seconds)
        
        # Sort by timestamp descending just in case
        # Timestamps in your log are integers (ms) e.g. 1768024911575
        fills.sort(key=lambda x: x.get('timestamp', 0), reverse=True)

        for fill in fills:
            # Check symbol match (your log has 'tradeable' or inside 'instrument'?)
            # The log you pasted structure: fill['event']['execution']['execution']['order']['tradeable'] == 'PF_XBTUSD'
            # OR sometimes simpler flat structure depending on API version. 
            # Let's try to extract safely.
            
            try:
                # Deep extraction based on your log structure
                execution = fill.get('event', {}).get('execution', {}).get('execution', {})
                fill_symbol = execution.get('order', {}).get('tradeable')
                fill_price_str = execution.get('price')
                fill_ts_ms = fill.get('timestamp')
            except:
                continue
                
            if fill_symbol != symbol:
                continue
                
            fill_dt = datetime.fromtimestamp(fill_ts_ms / 1000.0)
            
            if fill_dt > cutoff_time:
                return float(fill_price_str)

        return None
    except Exception as e:
        print(f"Error finding fill price for {symbol}: {e}")
        return None

# --- Main Loop ---
def main():
    if not all([GITHUB_TOKEN, KRAKEN_KEY, KRAKEN_SECRET]):
        print("Error: Missing environment variables.")
        return

    api = KrakenFuturesApi(KRAKEN_KEY, KRAKEN_SECRET)
    
    print("Starting monitor...")
    # Map: {symbol: {'size': 10, 'entryPrice': 50000}}
    previous_state = get_position_details(api)
    print(f"Initial state: {previous_state}")

    while True:
        try:
            time.sleep(30)
            
            current_state = get_position_details(api)
            changes_to_log = []
            timestamp = datetime.now().strftime("%H:%M:%S")

            all_symbols = set(current_state.keys()) | set(previous_state.keys())

            for sym in all_symbols:
                prev_data = previous_state.get(sym, {'size': 0.0, 'entryPrice': 0.0})
                curr_data = current_state.get(sym, {'size': 0.0, 'entryPrice': 0.0})
                
                old_qty = prev_data['size']
                new_qty = curr_data['size']
                old_entry = prev_data['entryPrice']

                if new_qty != old_qty:
                    # Determine PnL only if we REDUCED a position (Realized PnL)
                    # or flipped it.
                    # Simple PnL calc: (Exit - Entry) * Qty_Closed
                    
                    qty_delta = new_qty - old_qty
                    pnl_str = ""
                    
                    # We only calculate Realized PnL if size decreased in absolute terms (closed out)
                    # or crossed zero.
                    # If we just ADDED to a position, realized pnl is 0 (usually).
                    
                    is_reduction = abs(new_qty) < abs(old_qty)
                    is_flip = (new_qty * old_qty) < 0
                    is_close = (new_qty == 0 and old_qty != 0)

                    if (is_reduction or is_flip or is_close) and old_entry > 0:
                        # Fetch the execution price from fills
                        exec_price = get_recent_fill_price(api, sym, lookback_seconds=45)
                        
                        if exec_price:
                            # Calculate Amount Closed
                            # If Long (Pos > 0) and Delta < 0 -> We Sold.
                            #   PnL = (Exec - Entry) * Abs(Delta)
                            # If Short (Pos < 0) and Delta > 0 -> We Bought.
                            #   PnL = (Entry - Exec) * Abs(Delta)
                            
                            # Note: For flips, this is an approximation of the closed portion.
                            closed_qty = abs(qty_delta) 
                            # Refine closed_qty for flips? 
                            # If flipped +10 to -5, delta is -15. We only closed 10.
                            if is_flip:
                                closed_qty = abs(old_qty)
                            
                            calculated_pnl = 0.0
                            if old_qty > 0: # Long
                                calculated_pnl = (exec_price - old_entry) * closed_qty
                            else: # Short
                                calculated_pnl = (old_entry - exec_price) * closed_qty
                                
                            pnl_str = f"Profit: {calculated_pnl:.2f}"
                        else:
                            pnl_str = "Profit: ?? (No fill found)"
                    else:
                        # Increasing position = No Realized Profit
                        pnl_str = "Profit: 0.00 (Added)"

                    # Log Format
                    log_entry = f"Change {sym} {new_qty}/{old_qty} {pnl_str} {timestamp}"
                    print(log_entry)
                    changes_to_log.append(log_entry)

            if changes_to_log:
                append_to_github(changes_to_log)
                previous_state = current_state

        except KeyboardInterrupt:
            print("Stopping monitor.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
