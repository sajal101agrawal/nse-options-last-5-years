#!/usr/bin/env python3
"""
Download 5‑year daily close prices from Yahoo Finance for

1. NSE individual securities  (adds “.NS” suffix automatically)
2. NSE index futures / underlyings (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX …)
   – handled via an explicit symbol → Yahoo‑ticker map, or an optional
     'yahoo_symbol' field in your nse_fno_scripts.json.

Raw JSON dumps go to   <repo>/yahoo_finance/<SYMBOL>_raw.json  
Cleaned JSON (only date → underlying_price) goes to
   <repo>/yahoo_finance/<SYMBOL>_processed.json
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, Any

import requests

# --------------------------------------------------------------------------- #
#  Config & helpers                                                           #
# --------------------------------------------------------------------------- #

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_FILE = os.path.join(HERE, "nse_fno_scripts.json")
OUTPUT_DIR   = os.path.join(HERE, "yahoo_finance")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---- 5‑year date range ----------------------------------------------------- #
END_TS   = int(time.time())
START_TS = END_TS - 5 * 367 * 24 * 60 * 60          # five (++) years in seconds

# ---- Yahoo Finance request headers ---------------------------------------- #
HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/89.0.4389.82 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://finance.yahoo.com/",
    "Connection": "keep-alive",
}

# ---- Map common index futures to Yahoo tickers ---------------------------- #
INDEX_MAP: Dict[str, str] = {
    # symbol in your JSON    : Yahoo ticker
    "NIFTY":        "^NSEI",
    "BANKNIFTY":    "^NSEBANK",
    "FINNIFTY":     "^NSEFIN",
    "MIDCPNIFTY":   "^NIFMIDCP50",
    "SENSEX":       "^BSESN",
}


def resolve_yahoo_ticker(symbol: str, meta: Dict[str, Any] | None = None) -> str:
    """
    Decide which ticker to hit on Yahoo Finance.
    Priority:
      1. explicit 'yahoo_symbol' in JSON
      2. hard‑coded index mapping
      3. fallback to '<symbol>.NS'
    """
    if meta and meta.get("yahoo_symbol"):
        return meta["yahoo_symbol"]

    if symbol in INDEX_MAP:
        return INDEX_MAP[symbol]

    return f"{symbol}.NS"


# --------------------------------------------------------------------------- #
#  Main download loop                                                         #
# --------------------------------------------------------------------------- #

def main() -> None:
    with open(SCRIPTS_FILE, "r") as fh:
        script_data = json.load(fh)

    # Combine equities and index futures
    all_items: list[Dict[str, Any]] = (
        script_data.get("individual_securities", []) +
        script_data.get("index_futures", [])
    )

    for item in all_items:
        symbol = item["symbol"]
        ticker = resolve_yahoo_ticker(symbol, item)

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?interval=1d&period1={START_TS}&period2={END_TS}"
            f"&includeAdjustedClose=true&region=IN"
        )

        print(f"\n▼ {symbol}: downloading from "
              f"{datetime.fromtimestamp(START_TS).date()} "
              f"to {datetime.fromtimestamp(END_TS).date()}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            stock_data = resp.json()
        except Exception as exc:
            print(f"  ✖ request failed: {exc}")
            continue

        # ------------------- save raw -------------------------------------- #
        raw_path = os.path.join(OUTPUT_DIR, f"{symbol}_raw.json")
        with open(raw_path, "w") as fh:
            json.dump(stock_data, fh, indent=2)
        print(f"  ☑ raw JSON  → {raw_path}")

        # ------------------- process -------------------------------------- #
        processed = {
            "historical": {
                "scripts": {
                    symbol: {
                        "exchange": "NSE",
                        "segment": "Equity" if symbol not in INDEX_MAP else "Index",
                        "timestamps": {}
                    }
                }
            }
        }

        try:
            result = stock_data["chart"]["result"][0]
            ts_list = result.get("timestamp", [])
            closes  = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])

            for idx, ts in enumerate(ts_list):
                close_px = closes[idx] if idx < len(closes) else None
                if close_px is None:
                    continue
                date_key = datetime.fromtimestamp(ts).strftime("%d-%b-%Y")
                processed["historical"]["scripts"][symbol]["timestamps"][date_key] = {
                    "underlying_price": close_px
                }

        except (KeyError, IndexError, TypeError):
            print("  ⚠ could not parse data structure.")
            continue

        # save processed
        proc_path = os.path.join(OUTPUT_DIR, f"{symbol}_processed.json")
        with open(proc_path, "w") as fh:
            json.dump(processed, fh, indent=2)

        day_count = len(processed["historical"]["scripts"][symbol]["timestamps"])
        print(f"  ☑ cleaned   → {proc_path}  ({day_count} trading days)")

    print("\nDone.")


if __name__ == "__main__":
    main()
