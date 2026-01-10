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
    """
    Fetches the current file content from GitHub, appends new lines, 
    and updates the file via the GitHub REST API.
    """
    if not lines_to_add:
        return

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    # 1. Get current file content (to get SHA and existing data)
    r = requests.get(url, headers=headers)
    
    sha = None
    existing_content = ""

    if r.status_code == 200:
        data = r.json()
        sha = data.get("sha")
        # Content is base64 encoded
        existing_content = base64.b64decode(data.get("content", "")).decode("utf-8")
    elif r.status_code == 404:
        print(f"File {GITHUB_FILE_PATH} not found, creating new one.")
    else:
        print(f"Error fetching file from GitHub: {r.status_code} {r.text}")
        return

    # 2. Prepare new content
    # Ensure there is a newline at the end of existing content before appending
    if existing_content and not existing_content.endswith("\n"):
        existing_content += "\n"
    
    new_chunk = "\n".join(lines_to_add)
    full_content = existing_content + new_chunk
    
    # 3. Push update
    encoded_content = base64.b64encode(full_content.encode("utf-8")).decode("utf-8")
    
    payload = {
        "message": f"Update positions {datetime.now().strftime('%H:%M:%S')}",
        "content": encoded_content
    }
    if sha:
        payload["sha"] = sha

    put_resp = requests.put(url, headers=headers, json=payload)
    if put_resp.status_code in [200, 201]:
        print(f"Successfully logged {len(lines_to_add)} change(s) to GitHub.")
    else:
        print(f"Failed to update GitHub: {put_resp.status_code} {put_resp.text}")

# --- Kraken Helper ---
def get_position_map(api_client):
    """
    Fetches open positions and returns a dict {symbol: signed_size}.
    Longs are positive, shorts are negative.
    """
    try:
        response = api_client.get_open_positions()
        # The API usually returns a dict with a key 'openPositions' which is a list
        positions = response.get("openPositions", [])
        
        pos_map = {}
        for p in positions:
            symbol = p.get("symbol")
            size = float(p.get("size", 0.0))
            side = p.get("side", "long") # default to long if unspecified
            
            # Apply sign based on side
            if side == "short":
                size = -1 * abs(size)
            else:
                size = abs(size)
                
            pos_map[symbol] = size
        return pos_map
        
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return {}

def get_recent_fills_pnl(api_client, symbol, lookback_seconds=60):
    """
    Fetches recent fills and sums the realized PnL for the specific symbol
    occurred within the last 'lookback_seconds'.
    """
    try:
        # Get last 100 fills
        response = api_client.get_fills()
        fills = response.get("fills", [])
        
        total_pnl = 0.0
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(seconds=lookback_seconds)
        
        for fill in fills:
            fill_time_str = fill.get("fillTime")
            fill_symbol = fill.get("symbol")
            
            # Skip if symbol doesn't match
            if fill_symbol != symbol:
                continue

            try:
                # Basic ISO parsing
                fill_dt = datetime.strptime(fill_time_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S.%f")
            except ValueError:
                try:
                    fill_dt = datetime.strptime(fill_time_str.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
                except:
                    continue 

            if fill_dt > cutoff_time:
                # Add up realized PnL 
                pnl = float(fill.get("realizedPnl", 0.0))
                total_pnl += pnl
                
        return total_pnl

    except Exception as e:
        print(f"Error calculating PnL for {symbol}: {e}")
        return 0.0

# --- Main Loop ---
def main():
    if not all([GITHUB_TOKEN, KRAKEN_KEY, KRAKEN_SECRET]):
        print("Error: Missing environment variables (PAT, KRAKEN_FUTURES_KEY, or KRAKEN_FUTURES_SECRET).")
        return

    api = KrakenFuturesApi(KRAKEN_KEY, KRAKEN_SECRET)
    
    # --- STARTUP TEST: Check Fills ---
    print("--- Testing Connectivity & Fills Access ---")
    try:
        test_fills = api.get_fills()
        if 'fills' in test_fills:
            fill_list = test_fills['fills']
            print(f"Success: Retrieved {len(fill_list)} recent fills.")
            if len(fill_list) > 0:
                print(f"Latest fill sample: {fill_list[0]}")
            else:
                print("Note: No historical fills found on account.")
        else:
            print(f"Warning: Unexpected response from get_fills: {test_fills}")
    except Exception as e:
        print(f"CRITICAL: Failed to fetch fills. Check API Key permissions. Error: {e}")
    print("-------------------------------------------")

    # Initialize state
    print("Starting monitor...")
    previous_positions = get_position_map(api)
    print(f"Initial positions: {previous_positions}")

    while True:
        try:
            # Wait 30s
            time.sleep(30)
            
            # Fetch current state
            current_positions = get_position_map(api)
            
            changes_to_log = []
            timestamp = datetime.now().strftime("%H:%M:%S")

            # Identify all unique symbols involved
            all_symbols = set(current_positions.keys()) | set(previous_positions.keys())

            for sym in all_symbols:
                old_qty = previous_positions.get(sym, 0.0)
                new_qty = current_positions.get(sym, 0.0)

                if new_qty != old_qty:
                    # Calculate PnL for this change (lookback 40s to cover the sleep interval)
                    pnl_value = get_recent_fills_pnl(api, sym, lookback_seconds=40)
                    
                    # Log Format: Change [symbol] new/old PnL: [value] [time]
                    log_entry = f"Change {sym} {new_qty}/{old_qty} PnL: {pnl_value:.2f} {timestamp}"
                    print(log_entry)
                    changes_to_log.append(log_entry)

            # If we detected changes, push to GitHub
            if changes_to_log:
                append_to_github(changes_to_log)
                previous_positions = current_positions

        except KeyboardInterrupt:
            print("Stopping monitor.")
            break
        except Exception as e:
            print(f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    main()
