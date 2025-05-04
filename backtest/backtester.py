#!/usr/bin/env python3
# backtester.py - Implements the strangle backtesting strategy with delta hedging

import psycopg2
from psycopg2 import pool, extras, OperationalError, InterfaceError
import os
import logging
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv
import math
import time
from contextlib import contextmanager
import calendar

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5.5s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Environment/Config ---
load_dotenv()
ROOT_DIR = Path(__file__).resolve().parent

# --- DB Connection (Adapted from db_util.py) ---
_POOL: pool.SimpleConnectionPool | None = None
MAX_RETRY = 3 # Reduced retries for backtester reads compared to bulk writer

def _dsn() -> str:
    if dsn := os.getenv("PG_DSN"):
        return dsn
    return (
        "postgresql://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}".format(
            db=os.getenv("PG_DB", "postgres"), # Updated default DB name
            user=os.getenv("PG_USER", "postgres"),
            pwd=os.getenv("PG_PASSWORD", ""),
            host=os.getenv("PG_HOST", "localhost"),
            port=os.getenv("PG_PORT", 5432),
            ssl=os.getenv("PG_SSLMODE", "require"),
        )
    )

def _pool() -> pool.SimpleConnectionPool:
    global _POOL
    if _POOL is None:
        logging.info("Initializing connection pool...")
        _POOL = psycopg2.pool.SimpleConnectionPool(
            1,
            2, # Min/Max connections for backtester
            dsn=_dsn(),
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=10,
            keepalives_interval=5,
            keepalives_count=3,
        )
    return _POOL

@contextmanager
def _get_conn():
    conn = None
    tries, wait = 0, 1
    while True:
        try:
            conn = _pool().getconn()
            yield conn
            break # Success
        except (OperationalError, InterfaceError) as e:
            if conn: # Close poisoned connection
                try: _pool().putconn(conn, close=True)
                except Exception: pass
                conn = None
            tries += 1
            if tries > MAX_RETRY:
                logging.error(f"DB connection failed after {MAX_RETRY} retries: {e}")
                raise
            logging.warning(f"DB connection failed ({e}) - retry {tries} in {wait}s")
            time.sleep(wait)
            wait = min(wait * 2, 10)
        finally:
            if conn and not conn.closed:
                try: _pool().putconn(conn)
                except Exception: pass

# --- Helper Functions ---

def load_symbols_info(json_path: Path) -> tuple[list[str], set[str]]:
    """Loads all symbols and identifies index symbols from the JSON file."""
    if not json_path.exists():
        logging.error(f"Symbols file not found: {json_path}")
        return [], set()
    try:
        scripts = json.loads(json_path.read_text())
        index_symbols = {x["symbol"] for x in scripts.get("index_futures", [])}
        stock_symbols = {x["symbol"] for x in scripts.get("individual_securities", [])}
        all_symbols = sorted(list(index_symbols | stock_symbols))
        logging.info(f"Loaded {len(all_symbols)} symbols ({len(index_symbols)} indices). Example: {all_symbols[0]}")
        return all_symbols, index_symbols
    except Exception as e:
        logging.exception(f"Failed to load or parse symbols file: {json_path}")
        return [], set()

def is_index(symbol: str, index_symbols: set[str]) -> bool:
    """Checks if a symbol is an index."""
    return symbol in index_symbols

def get_trading_days_in_range(cursor, symbol: str, start_date: date, end_date: date) -> list[date]:
    """Fetches all trading days with data for a symbol within a date range."""
    query = """
        SELECT date FROM option_metrics
        WHERE symbol = %s AND date >= %s AND date <= %s
        ORDER BY date ASC;
    """
    try:
        cursor.execute(query, (symbol, start_date, end_date))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logging.error(f"Error fetching trading days for {symbol} between {start_date} and {end_date}: {e}")
        return []

def get_metrics_for_day(cursor, symbol: str, day: date) -> dict | None:
    """Fetches the full option_metrics record for a specific symbol and day."""
    query = """
        SELECT underlying_price, interest_rate, expiry_30d, upcoming_earning_date, option_chain
        FROM option_metrics
        WHERE symbol = %s AND date = %s;
    """
    try:
        cursor.execute(query, (symbol, day))
        row = cursor.fetchone()
        if row:
            expiry_30d = row[2]
            upcoming_earning_date = row[3]
            # Ensure underlying_price is float
            underlying_price = float(row[0]) if row[0] is not None else None
            return {
                "underlying_price": underlying_price,
                "interest_rate": row[1],
                "expiry_30d": expiry_30d,
                "upcoming_earning_date": upcoming_earning_date,
                "option_chain": row[4] # Already parsed as dict/list by psycopg2
            }
        else:
            # logging.warning(f"No metrics data found for {symbol} on {day}")
            return None
    except Exception as e:
        logging.error(f"Error fetching metrics for {symbol} on {day}: {e}")
        return None

def find_options_near_delta(option_chain: list, expiry_date: date, target_delta: float, option_type: str) -> dict | None:
    """Finds the option (CE or PE) closest to the target delta for a given expiry."""
    if not option_chain or not expiry_date:
        return None

    target_expiry_str = expiry_date.strftime("%d-%b-%Y")
    relevant_options = []
    for opt in option_chain:
        # Ensure necessary keys exist and delta is valid
        if (
            opt.get("type") == option_type
            and opt.get("expiry") == target_expiry_str
            and isinstance(opt.get("delta"), (int, float))
            and math.isfinite(opt["delta"])
            and isinstance(opt.get("strike"), (int, float))
            and isinstance(opt.get("settle"), (int, float))
        ):
            relevant_options.append(opt)

    if not relevant_options:
        return None

    best_option = min(
        relevant_options,
        key=lambda opt: abs(opt["delta"] - target_delta)
    )
    return best_option

def get_option_data(option_chain: list, expiry_date: date, strike_price: float, option_type: str) -> dict | None:
    """Extracts specific option details from the chain by expiry, strike, and type."""
    if not option_chain or not expiry_date or strike_price is None:
        return None
    target_expiry_str = expiry_date.strftime("%d-%b-%Y")
    for opt in option_chain:
        if (
            opt.get("type") == option_type
            and opt.get("expiry") == target_expiry_str
            and opt.get("strike") == strike_price
            # Check for price validity as well
            and isinstance(opt.get("settle"), (int, float))
            and math.isfinite(opt["settle"])
            and isinstance(opt.get("delta"), (int, float)) # Need delta too
            and math.isfinite(opt["delta"])
        ):
            return opt
    return None

def find_exit_day(cursor, symbol: str, expiry_date: date, is_index_flag: bool) -> date | None:
    """Finds the last Tue (index) or Wed (stock) trading day before expiry."""
    target_weekday = 1 if is_index_flag else 2 # 1=Tuesday, 2=Wednesday
    current_day = expiry_date - timedelta(days=1)
    for _ in range(14):
        cursor.execute("SELECT 1 FROM option_metrics WHERE symbol = %s AND date = %s LIMIT 1;", (symbol, current_day))
        is_trading_day = cursor.fetchone() is not None
        if is_trading_day:
            if current_day.weekday() == target_weekday:
                return current_day
            elif current_day.weekday() < target_weekday:
                return current_day
        current_day -= timedelta(days=1)
        if current_day < expiry_date - timedelta(days=20): break
    logging.warning(f"Could not find suitable exit day for {symbol} before expiry {expiry_date}")
    return None

def find_weekly_hedge_dates(entry_date: date, exit_date: date, trading_days_map: dict[date, bool]) -> list[date]:
    """Finds the last trading day of each week between entry and exit date."""
    hedge_dates = []
    current_date = entry_date + timedelta(days=(4 - entry_date.weekday() + 7) % 7) # First Friday >= entry_date
    if current_date <= entry_date: # If entry date is Fri/Sat/Sun, start from next week's Friday
        current_date += timedelta(days=7)

    while current_date <= exit_date:
        # Find the last trading day on or before this Friday
        hedge_day_candidate = current_date
        while hedge_day_candidate >= entry_date:
            if trading_days_map.get(hedge_day_candidate, False):
                # Ensure we don't add the same date twice if entry/exit is the hedge day
                if hedge_day_candidate > entry_date and hedge_day_candidate <= exit_date:
                     if not hedge_dates or hedge_day_candidate != hedge_dates[-1]:
                         hedge_dates.append(hedge_day_candidate)
                break
            hedge_day_candidate -= timedelta(days=1)
        current_date += timedelta(days=7) # Move to next Friday

    return sorted(list(set(hedge_dates))) # Ensure uniqueness and order

def insert_backtest_result(cursor, result_data: dict):
    """Inserts a single backtest result row, handling conflicts."""
    defaults = {
        "entry_credit": None, "exit_cost": None, "pnl_points": None,
        "skipped_reason": None, "call_entry_strike": None, "put_entry_strike": None,
        "call_entry_delta": None, "put_entry_delta": None, "call_entry_price": None,
        "put_entry_price": None, "call_exit_price": None, "put_exit_price": None,
        "exit_date": None, "hedge_pnl_points": None # Added hedge PNL
    }
    for key, default_val in defaults.items():
        result_data.setdefault(key, default_val)

    # Ensure numeric types are valid or None
    numeric_fields = [
        "entry_credit", "exit_cost", "pnl_points", "call_entry_strike",
        "put_entry_strike", "call_entry_delta", "put_entry_delta",
        "call_entry_price", "put_entry_price", "call_exit_price",
        "put_exit_price", "hedge_pnl_points"
    ]
    for field in numeric_fields:
        if field in result_data and isinstance(result_data[field], (int, float)) and not math.isfinite(result_data[field]):
            result_data[field] = None

    query = """
    INSERT INTO backtest_results (
        symbol, entry_date, exit_date, year, month,
        entry_credit, exit_cost, pnl_points, skipped_reason,
        call_entry_strike, put_entry_strike, call_entry_delta, put_entry_delta,
        call_entry_price, put_entry_price, call_exit_price, put_exit_price,
        hedge_pnl_points -- Added hedge PNL column
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, year, month) DO UPDATE SET -- Changed to UPDATE on conflict
        entry_date = EXCLUDED.entry_date,
        exit_date = EXCLUDED.exit_date,
        entry_credit = EXCLUDED.entry_credit,
        exit_cost = EXCLUDED.exit_cost,
        pnl_points = EXCLUDED.pnl_points,
        skipped_reason = EXCLUDED.skipped_reason,
        call_entry_strike = EXCLUDED.call_entry_strike,
        put_entry_strike = EXCLUDED.put_entry_strike,
        call_entry_delta = EXCLUDED.call_entry_delta,
        put_entry_delta = EXCLUDED.put_entry_delta,
        call_entry_price = EXCLUDED.call_entry_price,
        put_entry_price = EXCLUDED.put_entry_price,
        call_exit_price = EXCLUDED.call_exit_price,
        put_exit_price = EXCLUDED.put_exit_price,
        hedge_pnl_points = EXCLUDED.hedge_pnl_points;
    """
    params = (
        result_data["symbol"], result_data["entry_date"], result_data["exit_date"],
        result_data["year"], result_data["month"],
        result_data["entry_credit"], result_data["exit_cost"], result_data["pnl_points"],
        result_data["skipped_reason"],
        result_data["call_entry_strike"], result_data["put_entry_strike"],
        result_data["call_entry_delta"], result_data["put_entry_delta"],
        result_data["call_entry_price"], result_data["put_entry_price"],
        result_data["call_exit_price"], result_data["put_exit_price"],
        result_data["hedge_pnl_points"] # Added hedge PNL param
    )
    try:
        cursor.execute(query, params)
    except Exception as e:
        logging.error(f"Failed to insert/update result for {result_data.get('symbol')} {result_data.get('year')}-{result_data.get('month')}: {e}")
        logging.error(f"Query: {cursor.mogrify(query, params)}") # Log the query and params

# --- Main Backtest Function ---

def run_backtest_month(symbol: str, year: int, month: int, index_symbols: set[str], cursor):
    """Runs the backtest strategy for a single symbol/month/year."""
    logging.info(f"--- Running backtest for {symbol} {year}-{month:02d} ---")
    result = {"symbol": symbol, "year": year, "month": month}

    # Get all trading days for the month to check entry and build map for hedging dates
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    all_trading_days_in_month = get_trading_days_in_range(cursor, symbol, month_start, month_end)

    if not all_trading_days_in_month:
        logging.warning(f"No trading days found for {symbol} {year}-{month}. Skipping.")
        result["skipped_reason"] = "No Trading Days"
        insert_backtest_result(cursor, result)
        return

    entry_date = all_trading_days_in_month[0]
    result["entry_date"] = entry_date
    logging.info(f"Entry Date: {entry_date}")

    entry_metrics = get_metrics_for_day(cursor, symbol, entry_date)
    if not entry_metrics or not entry_metrics.get("option_chain") or not entry_metrics.get("expiry_30d") or entry_metrics.get("underlying_price") is None:
        logging.warning(f"Missing entry metrics/chain/expiry/price for {symbol} on {entry_date}. Skipping.")
        result["skipped_reason"] = "Missing Entry Data"
        insert_backtest_result(cursor, result)
        return

    expiry_30d = entry_metrics["expiry_30d"]
    upcoming_earning_date = entry_metrics.get("upcoming_earning_date")
    if upcoming_earning_date and entry_date <= upcoming_earning_date <= expiry_30d:
        logging.info(f"Earnings conflict: Earning date {upcoming_earning_date} between {entry_date} and {expiry_30d}. Skipping.")
        result["skipped_reason"] = f"Earnings on {upcoming_earning_date}"
        insert_backtest_result(cursor, result)
        return

    entry_option_chain = entry_metrics["option_chain"]
    call_option = find_options_near_delta(entry_option_chain, expiry_30d, 0.20, "CE")
    put_option = find_options_near_delta(entry_option_chain, expiry_30d, -0.20, "PE")

    if not call_option or not put_option:
        logging.warning(f"Could not find suitable CE/PE options near 20 delta for {symbol} on {entry_date}. Skipping.")
        result["skipped_reason"] = "No Suitable Options"
        insert_backtest_result(cursor, result)
        return

    result["call_entry_strike"] = call_option["strike"]
    result["put_entry_strike"] = put_option["strike"]
    result["call_entry_delta"] = call_option["delta"]
    result["put_entry_delta"] = put_option["delta"]
    result["call_entry_price"] = call_option["settle"]
    result["put_entry_price"] = put_option["settle"]
    result["entry_credit"] = call_option["settle"] + put_option["settle"]
    logging.info(f"Entry: Sell CE {call_option['strike']} @ {call_option['settle']:.2f} (Δ {call_option['delta']:.2f}), Sell PE {put_option['strike']} @ {put_option['settle']:.2f} (Δ {put_option['delta']:.2f}). Credit: {result['entry_credit']:.2f}")

    is_index_flag = is_index(symbol, index_symbols)
    exit_date = find_exit_day(cursor, symbol, expiry_30d, is_index_flag)
    if not exit_date:
        logging.warning(f"Could not determine exit date for {symbol} before {expiry_30d}. Skipping trade result.")
        result["skipped_reason"] = "Cannot Find Exit Date"
        insert_backtest_result(cursor, result)
        return
    result["exit_date"] = exit_date
    logging.info(f"Exit Date: {exit_date}")

    # --- Delta Hedging Logic --- 
    current_hedge_position = 0.0
    total_hedge_pnl = 0.0
    last_hedge_price = entry_metrics["underlying_price"] # Price at which current position was established/last hedged

    # Get all trading days between entry and exit for hedge date finding
    all_trading_days_in_trade = get_trading_days_in_range(cursor, symbol, entry_date, exit_date)
    trading_days_map = {day: True for day in all_trading_days_in_trade}

    weekly_hedge_dates = find_weekly_hedge_dates(entry_date, exit_date, trading_days_map)
    logging.info(f"Hedging Dates: {weekly_hedge_dates}")

    for hedge_date in weekly_hedge_dates:
        if hedge_date >= exit_date: # Don't hedge on or after exit day
            continue

        hedge_metrics = get_metrics_for_day(cursor, symbol, hedge_date)
        if not hedge_metrics or hedge_metrics.get("underlying_price") is None or not hedge_metrics.get("option_chain"):
            logging.warning(f"Missing metrics/price/chain for hedge on {hedge_date}. Skipping hedge.")
            continue

        hedge_price = hedge_metrics["underlying_price"]
        hedge_option_chain = hedge_metrics["option_chain"]

        # Calculate PnL from existing hedge position before adjusting
        if current_hedge_position != 0:
            price_change = hedge_price - last_hedge_price
            pnl_from_position = current_hedge_position * price_change
            total_hedge_pnl += pnl_from_position
            # logging.debug(f"Hedge PNL ({hedge_date}): Pos={current_hedge_position:.2f}, PriceChange={price_change:.2f}, PNL={pnl_from_position:.2f}, Total={total_hedge_pnl:.2f}")

        # Get current deltas of the strangle legs
        call_data_hedge = get_option_data(hedge_option_chain, expiry_30d, result["call_entry_strike"], "CE")
        put_data_hedge = get_option_data(hedge_option_chain, expiry_30d, result["put_entry_strike"], "PE")

        current_call_delta = call_data_hedge["delta"] if call_data_hedge else 0.0
        current_put_delta = put_data_hedge["delta"] if put_data_hedge else 0.0

        # Portfolio Delta = - (Call Delta + Put Delta) (from short positions)
        strangle_net_delta = - (current_call_delta + current_put_delta)

        hedge_position_delta = current_hedge_position # Delta of underlying is 1
        total_portfolio_delta = strangle_net_delta + hedge_position_delta

        required_hedge_adjustment = -total_portfolio_delta # Amount of underlying to buy/sell

        if abs(required_hedge_adjustment) > 0.001: # Avoid tiny adjustments
            # Simulate hedge trade - PnL is realized when position changes or is closed
            # Cost basis adjustment happens here
            logging.info(f"Hedge ({hedge_date}): StrangleΔ={strangle_net_delta:.2f}, PortΔ={total_portfolio_delta:.2f}, Adjusting by {required_hedge_adjustment:.2f} @ {hedge_price:.2f}")
            current_hedge_position += required_hedge_adjustment
            last_hedge_price = hedge_price # Update price basis for the new position
        else:
            # Update price basis even if no trade, for next PnL calculation
            last_hedge_price = hedge_price
            # logging.debug(f"Hedge ({hedge_date}): No adjustment needed. PortΔ={total_portfolio_delta:.2f}")

    # --- End Delta Hedging Loop ---

    # Get exit metrics
    exit_metrics = get_metrics_for_day(cursor, symbol, exit_date)
    if not exit_metrics or exit_metrics.get("underlying_price") is None or not exit_metrics.get("option_chain"):
        logging.warning(f"Missing exit metrics/price/chain for {symbol} on {exit_date}. Cannot calculate final PNL.")
        result["skipped_reason"] = "Missing Exit Data"
        insert_backtest_result(cursor, result)
        return

    exit_price_underlying = exit_metrics["underlying_price"]

    # Liquidate final hedge position
    if current_hedge_position != 0:
        price_change = exit_price_underlying - last_hedge_price
        pnl_from_position = current_hedge_position * price_change
        total_hedge_pnl += pnl_from_position
        logging.info(f"Hedge Liquidation ({exit_date}): Closing {current_hedge_position:.2f} @ {exit_price_underlying:.2f}, PNL={pnl_from_position:.2f}, Total Hedge PNL={total_hedge_pnl:.2f}")
        current_hedge_position = 0 # Reset position

    result["hedge_pnl_points"] = total_hedge_pnl

    # Find exit prices for strangle
    exit_option_chain = exit_metrics["option_chain"]
    call_exit_data = get_option_data(exit_option_chain, expiry_30d, result["call_entry_strike"], "CE")
    put_exit_data = get_option_data(exit_option_chain, expiry_30d, result["put_entry_strike"], "PE")

    if not call_exit_data or not put_exit_data:
        logging.warning(f"Could not find exit prices for sold options {symbol} on {exit_date}. Cannot calculate Strangle PNL.")
        result["skipped_reason"] = "Missing Exit Price"
        # Keep hedge PNL if calculated
        insert_backtest_result(cursor, result)
        return

    result["call_exit_price"] = call_exit_data["settle"]
    result["put_exit_price"] = put_exit_data["settle"]
    result["exit_cost"] = result["call_exit_price"] + result["put_exit_price"]

    strangle_pnl_points = result["entry_credit"] - result["exit_cost"]
    result["pnl_points"] = strangle_pnl_points + total_hedge_pnl # Combined PNL

    logging.info(f"Exit Strangle: Buy CE {result['call_entry_strike']} @ {result['call_exit_price']:.2f}, Buy PE {result['put_entry_strike']} @ {result['put_exit_price']:.2f}. Cost: {result['exit_cost']:.2f}")
    logging.info(f"Result: Strangle PNL={strangle_pnl_points:.2f}, Hedge PNL={total_hedge_pnl:.2f}, Total PNL = {result['pnl_points']:.2f} points")

    insert_backtest_result(cursor, result)

# --- Main Execution Logic ---

def main():
    SYMBOLS_JSON = ROOT_DIR.parent / "nse_fno_scripts.json"
    if not SYMBOLS_JSON.exists():
       logging.error(f"{SYMBOLS_JSON} not found.")
       return

    all_symbols, index_symbols = load_symbols_info(SYMBOLS_JSON)
    if not all_symbols:
        return

    # Define backtest period (example: last 4 years)
    end_year = datetime.now().year
    start_year = end_year - 4

    # --- MODIFY HERE FOR TESTING --- #
    # Example: Run only for HDFCBANK
    symbols_to_run = ["HDFCBANK"]
    years_to_run = [2022, 2023, 2024, 2025]
    # symbols_to_run = all_symbols # Run for all symbols
    # years_to_run = range(start_year, end_year + 1)
    months_to_run = range(1, 13) # All months
    # --- END MODIFY --- #

    processed_count = 0
    total_tasks = len(symbols_to_run) * len(years_to_run) * len(months_to_run)

    try:
        with _get_conn() as conn:
            logging.info("Database connection successful.")
            for symbol in symbols_to_run:
                for year in years_to_run:
                    for month in months_to_run:
                        try:
                            with conn.cursor() as cur:
                                run_backtest_month(symbol, year, month, index_symbols, cur)
                            conn.commit() # Commit after each month
                        except Exception as e:
                            logging.exception(f"Error during backtest for {symbol} {year}-{month}. Rolling back month.")
                            try: conn.rollback()
                            except Exception as rb_e:
                                logging.error(f"Rollback failed: {rb_e}")
                        processed_count += 1
                        if processed_count % 10 == 0 or processed_count == total_tasks:
                            logging.info(f"Progress: {processed_count} / {total_tasks} months processed.")
            logging.info(f"Backtest finished. Processed {processed_count} months.")
    except Exception as e:
        logging.exception("An error occurred during the backtesting process.")
    finally:
        if _POOL:
            _POOL.closeall()
            logging.info("Connection pool closed.")

if __name__ == "__main__":
    if not ROOT_DIR.exists():
        ROOT_DIR.mkdir(parents=True, exist_ok=True)
    main()


