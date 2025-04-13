import sys
import os
import json
from datetime import datetime, timedelta
import time
import requests

# Calculate date range for past 5 years
end_date = int(time.time())
start_date = end_date - (5 * 365 * 24 * 60 * 60)  # 5 years in seconds

# Define the script to download
script = "HDFCBANK.NS"
symbol = script

# Yahoo Finance URL
url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&period1={start_date}&period2={end_date}&includeAdjustedClose=true&region=IN"

# Send request to Yahoo Finance
print(f"Downloading data for {script} from {datetime.fromtimestamp(start_date)} to {datetime.fromtimestamp(end_date)}")
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

response = requests.get(url, headers=headers)

if response.status_code != 200:
    print(f"Failed to fetch data: {response.status_code}")
    sys.exit(1)

stock_data = response.json()

# Create output directory if it doesn't exist
output_dir = os.path.dirname(os.path.abspath(__file__))
os.makedirs(output_dir, exist_ok=True)

# Save the raw data
output_file = os.path.join(output_dir, f"{script.replace('.', '_')}_raw.json")
with open(output_file, 'w') as f:
    json.dump(stock_data, f, indent=2)
print(f"Raw data saved to {output_file}")

# Process the data into the required format
processed_data = {"historical": {"scripts": {}}}
script_name = script.split('.')[0]  # Remove the .NS suffix

processed_data["historical"]["scripts"][script_name] = {
    "exchange": "NSE",
    "segment": "Equity",
    "timestamps": {}
}

# Extract timestamp and price data
if 'chart' in stock_data and 'result' in stock_data['chart'] and stock_data['chart']['result']:
    result = stock_data['chart']['result'][0]
    
    timestamps = result.get('timestamp', [])
    indicators = result.get('indicators', {})
    quotes = indicators.get('quote', [{}])[0]
    
    for i, ts in enumerate(timestamps):
        # Convert timestamp to date string
        date_str = datetime.fromtimestamp(ts).strftime('%d-%b-%Y')
        
        # Get price data for this timestamp
        underlying_price = quotes.get('close', [])[i] if i < len(quotes.get('close', [])) else None
        
        if underlying_price is not None:
            processed_data["historical"]["scripts"][script_name]["timestamps"][date_str] = {
                "underlying_price": underlying_price,
                # Other fields will be populated from Bhavcopy data
            }

# Save the processed data
processed_file = os.path.join(output_dir, f"{script_name}_processed.json")
with open(processed_file, 'w') as f:
    json.dump(processed_data, f, indent=2)
print(f"Processed data saved to {processed_file}")

# Print summary
print(f"Downloaded {len(processed_data['historical']['scripts'][script_name]['timestamps'])} days of data for {script}")
