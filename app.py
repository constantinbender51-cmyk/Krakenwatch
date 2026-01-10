import os
from kraken_futures import KrakenFuturesApi

key = os.getenv("KRAKEN_FUTURES_KEY")
secret = os.getenv("KRAKEN_FUTURES_SECRET")
api = KrakenFuturesApi(key, secret)

response = api.get_fills()
if 'fills' in response and len(response['fills']) > 0:
    # Print ONLY the keys (field names) of the most recent fill
    print("Found fields:", list(response['fills'][0].keys()))
else:
    print("No fills found or API error:", response)
