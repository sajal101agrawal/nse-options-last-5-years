import sys
import os
import json
from datetime import datetime, timedelta
import time
import requests

# Constants
SCRIPTS_FILE = "nse_fno_scripts.json"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "yahoo_finance")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Calculate date range for past 5 years
end_date = int(time.time())
start_date = end_date - (5 * 365 * 24 * 60 * 60)  # 5 years in seconds

# Read script list from JSON
with open(SCRIPTS_FILE, "r") as f:
    script_data = json.load(f)

scripts = script_data.get("individual_securities", [])

# Headers for Yahoo Finance request
headers = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/89.0.4389.82 Safari/537.36'
    ),
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://finance.yahoo.com/',
    'Connection': 'keep-alive',
}

for item in scripts:
    symbol = item["symbol"]
    script = f"{symbol}.NS"

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{script}?interval=1d&period1={start_date}&period2={end_date}&includeAdjustedClose=true&region=IN"
    print(f"\nDownloading data for {script} from {datetime.fromtimestamp(start_date)} to {datetime.fromtimestamp(end_date)}")

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch data for {symbol}: {response.status_code}")
            continue

        stock_data = response.json()

        # Save raw data
        raw_file = os.path.join(OUTPUT_DIR, f"{symbol}_raw.json")
        with open(raw_file, 'w') as f:
            json.dump(stock_data, f, indent=2)
        print(f"Raw data saved to {raw_file}")

        # Processed structure
        processed_data = {"historical": {"scripts": {}}}
        processed_data["historical"]["scripts"][symbol] = {
            "exchange": "NSE",
            "segment": "Equity",
            "timestamps": {}
        }

        if 'chart' in stock_data and 'result' in stock_data['chart'] and stock_data['chart']['result']:
            result = stock_data['chart']['result'][0]
            timestamps = result.get('timestamp', [])
            quotes = result.get('indicators', {}).get('quote', [{}])[0]

            for i, ts in enumerate(timestamps):
                date_str = datetime.fromtimestamp(ts).strftime('%d-%b-%Y')
                close_prices = quotes.get('close', [])
                if i < len(close_prices) and close_prices[i] is not None:
                    processed_data["historical"]["scripts"][symbol]["timestamps"][date_str] = {
                        "underlying_price": close_prices[i]
                    }

        # Save processed data
        processed_file = os.path.join(OUTPUT_DIR, f"{symbol}_processed.json")
        with open(processed_file, 'w') as f:
            json.dump(processed_data, f, indent=2)
        print(f"Processed data saved to {processed_file}")
        print(f"Downloaded {len(processed_data['historical']['scripts'][symbol]['timestamps'])} days of data")

    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")
