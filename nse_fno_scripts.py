"""
Script to save NSE F&O scripts in json from the existing list of index futures and individual securities.
"""

import json

# List of index futures
index_futures = [
    {"symbol": "NIFTY", "name": "Nifty 50"},
    {"symbol": "BANKNIFTY", "name": "Nifty BANK"},
    {"symbol": "FINNIFTY", "name": "Nifty FINANCIAL SERVICES"},
    {"symbol": "MIDCPNIFTY", "name": "Nifty MIDCAP SELECT"},
    {"symbol": "NIFTYNXT50", "name": "Nifty NEXT 50"}
]

# List of individual securities from NSE F&O segment
individual_securities = [
    {"symbol": "ABB", "name": "ABB India Limited"},
    {"symbol": "ACC", "name": "ACC Limited"},
    {"symbol": "APLAPOLLO", "name": "APL Apollo Tubes Limited"},
    {"symbol": "AUBANK", "name": "AU Small Finance Bank Limited"},
    {"symbol": "AARTIIND", "name": "Aarti Industries Limited"},
    {"symbol": "ADANIENSOL", "name": "Adani Energy Solutions Limited"},
    {"symbol": "ADANIENT", "name": "Adani Enterprises Limited"},
    {"symbol": "ADANIGREEN", "name": "Adani Green Energy Limited"},
    {"symbol": "ADANIPORTS", "name": "Adani Ports and Special Economic Zone Limited"},
    {"symbol": "ATGL", "name": "Adani Total Gas Limited"},
    {"symbol": "ABFRL", "name": "Aditya Birla Fashion and Retail Limited"},
    {"symbol": "ALKEM", "name": "Alkem Laboratories Limited"},
    {"symbol": "AMBUJACEM", "name": "Ambuja Cements Limited"},
    {"symbol": "HDFCBANK", "name": "HDFC Bank Limited"}
]

# Combine all F&O scripts
all_fno_scripts = {
    "index_futures": index_futures,
    "individual_securities": individual_securities
}

# Save to JSON file
with open('nse_fno_scripts.json', 'w') as f:
    json.dump(all_fno_scripts, f, indent=2)

print(f"Saved {len(index_futures)} index futures and {len(individual_securities)} individual securities to nse_fno_scripts.json")

# Create a list of symbols for further processing
symbols = [item["symbol"] for item in index_futures] + [item["symbol"] for item in individual_securities]
print(f"Total F&O scripts: {len(symbols)}")
print(f"Symbols: {', '.join(symbols)}")
