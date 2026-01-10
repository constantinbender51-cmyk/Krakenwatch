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
        # UPDATED REGEX:
        # 1. Matches asset name inside PF_...USD
        # 2. Skips the quantity part (.*? matches the size/size part)
        # 3. Captures the specific number after "Profit: "
        match = re.search(r'Change PF_([A-Z0-9]+)USD\s+.*?Profit:\s+([-\d\.]+)', line)
        
        if match:
            asset = match.group(1) # Extracts 'DOT' from 'PF_DOTUSD'
            
            try:
                # Directly use the profit calculated by the logger script
                profit = float(match.group(2))
                
                if asset not in stats:
                    stats[asset] = {'P': 0.0, 'T': 0, 'Wins': 0}
                
                stats[asset]['P'] += profit
                stats[asset]['T'] += 1
                
                # Check for win based on the explicit profit value
                if profit > 0:
                    stats[asset]['Wins'] += 1
                    
            except ValueError:
                continue
                
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
            margin: 0;
        }
        h1 {
            text-align: center;
            letter-spacing: 15px;
            text-transform: uppercase;
            font-weight: normal;
            margin-bottom: 60px;
            font-size: 32px;
        }
        table {
            border-collapse: collapse;
            border: none;
            font-size: 18px;
            /* Fixed layout ensures columns don't shift based on content length */
            table-layout: fixed;
            width: 700px; 
        }
        th {
            font-weight: normal;
            color: #888;
            padding-bottom: 20px;
            text-align: center;
        }
        td {
            padding: 15px 0;
            text-align: center;
            border: none;
        }
        
        /* PTA Alignment Logic:
           We divide the table into 4 equal columns (25% each).
           This ensures P, T, and A take up equal visual space.
        */
        th, td {
            width: 25%;
        }

        /* The Asset Names column (last column) styling */
        td:last-child {
            font-weight: bold;
            text-align: left;
            /* Padding ensures the text starts slightly offset from the center of the column */
            padding-left: 50px; 
            box-sizing: border-box;
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
    
    # Sort assets alphabetically
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
        <title>Primate</title>
        {css}
        <meta http-equiv="refresh" content="60">
    </head>
    <body>
        <h1>Primate</h1>
        
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
