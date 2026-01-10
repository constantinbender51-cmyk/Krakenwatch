import http.server
import socketserver
import os
import re
import urllib.request
import urllib.error

# --- Configuration ---
# The raw URL for your specific file on GitHub
GITHUB_URL_MAIN = 'https://raw.githubusercontent.com/constantinbender51-cmyk/Models/main/kraken_logs.txt'
GITHUB_URL_MASTER = 'https://raw.githubusercontent.com/constantinbender51-cmyk/Models/master/kraken_logs.txt'

PORT = int(os.environ.get('PORT', 8000))

def fetch_logs():
    """Fetches the log file content directly from GitHub."""
    urls_to_try = [GITHUB_URL_MAIN, GITHUB_URL_MASTER]
    
    for url in urls_to_try:
        try:
            with urllib.request.urlopen(url) as response:
                return response.read().decode('utf-8')
        except urllib.error.HTTPError:
            continue # Try next URL if 404
        except Exception as e:
            print(f"Error fetching logs: {e}")
            return ""
    return ""

def parse_logs(log_data):
    stats = {}
    
    lines = log_data.split('\n')
    for line in lines:
        # Regex to extract: Asset, New Val, Old Val
        # Looks for: Change PF_[Asset] [New]/[Old]
        # Example: Change PF_DOTUSD 22.4/22.2
        match = re.search(r'Change PF_([A-Z0-9]+)USD\s+([-\d\.]+)/([-\d\.]+)', line)
        
        if match:
            asset = match.group(1) # Extracts 'DOT' from 'PF_DOTUSD'
            new_val = float(match.group(2))
            old_val = float(match.group(3))
            
            profit = new_val - old_val
            
            if asset not in stats:
                stats[asset] = {'P': 0.0, 'T': 0, 'Wins': 0}
            
            stats[asset]['P'] += profit
            stats[asset]['T'] += 1
            if profit > 0:
                stats[asset]['Wins'] += 1
                
    return stats

def generate_html_content(stats):
    # CSS Styles for the simplistic design
    css = """
    <style>
        body {
            font-family: Helvetica, Arial, sans-serif;
            background-color: white;
            color: black;
            display: flex;
            flex-direction: column;
            align-items: center;
            padding-top: 50px;
        }
        h1 {
            text-align: center;
            letter-spacing: 15px;
            text-transform: uppercase;
            font-weight: normal;
            margin-bottom: 60px;
        }
        table {
            border-collapse: collapse;
            border: none;
            font-size: 18px;
        }
        th, td {
            padding: 15px 30px;
            text-align: center;
            border: none;
        }
        /* The Asset Names column (last column) styling */
        td:last-child {
            font-weight: bold;
            text-align: left;
            padding-left: 40px;
        }
        /* The Total Row styling */
        tr.total-row td {
            padding-top: 40px;
            font-weight: bold;
            font-size: 20px;
        }
    </style>
    """
    
    # Calculate Totals
    total_p = 0.0
    total_t = 0
    total_wins = 0
    
    rows_html = ""
    
    # Sort assets alphabetically if desired, or keep insertion order
    for asset in sorted(stats.keys()):
        data = stats[asset]
        # Calculate Accuracy for this asset
        acc = (data['Wins'] / data['T'] * 100) if data['T'] > 0 else 0
        
        # Accumulate globals
        total_p += data['P']
        total_t += data['T']
        total_wins += data['Wins']
        
        # Format Row: P | T | A | Asset
        rows_html += f"""
        <tr>
            <td>{data['P']:.2f}</td>
            <td>{data['T']}</td>
            <td>{acc:.1f}%</td>
            <td>{asset}</td>
        </tr>
        """

    # Calculate Global Accuracy
    total_acc = (total_wins / total_t * 100) if total_t > 0 else 0
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Trading Summary</title>
        {css}
        <meta http-equiv="refresh" content="60">
    </head>
    <body>
        <h1>Trade View</h1>
        
        <table>
            <tr>
                <th>P</th>
                <th>T</th>
                <th>A</th>
                <th></th>
            </tr>
            
            {rows_html}
            
            <tr class="total-row">
                <td>{total_p:.2f}</td>
                <td>{total_t}</td>
                <td>{total_acc:.1f}%</td>
                <td>Total</td>
            </tr>
        </table>
    </body>
    </html>
    """
    return html

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # 1. Fetch live data
        log_data = fetch_logs()
        
        # 2. Parse data
        stats = parse_logs(log_data)
        
        # 3. Generate HTML
        html_content = generate_html_content(stats)
        
        # 4. Send Response
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(html_content.encode('utf-8'))

def run_server():
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Serving at port {PORT}")
        httpd.serve_forever()

if __name__ == "__main__":
    run_server()
