# Options Backtesting Project (with Delta Hedging)

## Overview

This project implements a backtesting system for a specific options trading strategy based on historical data stored in a PostgreSQL database. The system analyzes historical options data, applies the defined strategy rules month by month for specified symbols (including weekly delta hedging), and stores the performance results (Profit/Loss) in a dedicated database table.

The primary data source is the `option_metrics` table, which is assumed to be populated by an ETL process (like the provided `store_s.py`) containing daily options chain data, underlying prices, interest rates, calculated IV, Greeks, and expiry dates.

## Strategy Implemented

The backtested strategy is a **Short Strangle with Weekly Delta Hedging**:

1.  **Entry:**
    *   **Timing:** On the first trading day of each calendar month.
    *   **Instruments:** Sell one Call option and one Put option.
    *   **Expiry:** Target the monthly expiry with approximately 30 Days To Expiration (DTE), identified using the `expiry_30d` field from the database.
    *   **Strike Selection:**
        *   Call: Select the strike closest to +0.20 delta.
        *   Put: Select the strike closest to -0.20 delta.
    *   **Quantity:** 1 lot each (calculations are done in points per strangle).
2.  **Earnings:**
    *   The trade is **skipped** for the month if the underlying symbol has a scheduled earnings announcement (`upcoming_earning_date` in the database) between the entry date and the option expiry date (`expiry_30d`).
3.  **Hedging:**
    *   **Frequency:** Weekly.
    *   **Timing:** Performed on the last trading day of each week (typically Friday, or earlier if Friday is a holiday) between the entry date and the exit date.
    *   **Goal:** Maintain portfolio delta neutrality.
    *   **Instrument:** Uses the **underlying spot asset** (price from `underlying_price` in `option_metrics`) for hedging.
    *   **Logic:**
        *   Calculates the net delta of the short strangle position (using deltas from `option_chain`).
        *   Calculates the total portfolio delta (strangle delta + delta from existing hedge position).
        *   Determines the required adjustment in the underlying asset to bring the total portfolio delta back to zero.
        *   Simulates buying or selling the required quantity of the underlying asset at the closing price (`underlying_price`) on the hedge date.
        *   Tracks the accumulated Profit/Loss from these hedge trades (`hedge_pnl_points`).
    *   **Liquidation:** Any remaining hedge position is liquidated at the underlying price on the final `exit_date`.
4.  **Exit:**
    *   **Timing:** Close the initial strangle position before expiry on a specific day:
        *   **Indices (e.g., NIFTY):** Last trading Tuesday before the `expiry_30d` date.
        *   **Stocks:** Last trading Wednesday before the `expiry_30d` date.
    *   **Holiday Handling:** If the target exit day is a holiday (no data in `option_metrics`), the position is closed on the preceding trading day.

## Setup

1.  **Dependencies:**
    *   Python 3.11+
    *   Required Python packages: `psycopg2-binary`, `python-dotenv`.
    *   Install using pip: `python3.11 -m pip install psycopg2-binary python-dotenv`
2.  **Database:**
    *   A PostgreSQL database server is required.
    *   The database must contain the `option_metrics` table populated with historical options data.
    *   The `backtest_results` table needs to be created (see `create_results_table.sql`) and potentially altered to include the hedging column (see `alter_results_table.sql`). Use `execute_sql.py` to run these scripts.
3.  **Environment Variables:**
    *   The scripts (`backtester_hedged.py`, `execute_sql.py`) require database connection details. These are best provided via environment variables:
        *   `PG_HOST`: Database server hostname or IP address.
        *   `PG_PORT`: Database server port (default: 5432).
        *   `PG_DB`: Database name (e.g., `postgres`).
        *   `PG_USER`: Database username.
        *   `PG_PASSWORD`: Database password.
        *   `PG_SSLMODE`: SSL mode (e.g., `require`, `prefer`, `disable`).
    *   Alternatively, a full DSN string can be provided via the `PG_DSN` environment variable.
4.  **Symbol List:**
    *   The `backtester_hedged.py` script requires a `nse_fno_scripts.json` file in the same directory (or parent directory) to identify symbols and distinguish between stocks and indices (for exit rule differences).
    *   The file format should be:
        ```json
        {
          "index_futures": [
            {"symbol": "NIFTY"},
            {"symbol": "BANKNIFTY"}
          ],
          "individual_securities": [
            {"symbol": "RELIANCE"},
            {"symbol": "INFY"}
            // ... other stock symbols
          ]
        }
        ```

## Running the Backtester

1.  **Ensure Setup:** Verify all dependencies are installed, environment variables are set, the database is accessible with the correct table structure, and the `nse_fno_scripts.json` file exists and is populated.
2.  **Configure Scope (Optional):**
    *   By default, the script (`backtester_hedged.py`) is configured to run for the last 4 years for all symbols found in `nse_fno_scripts.json`.
    *   To change the scope (e.g., for testing), edit the `main()` function in `backtester_hedged.py`:
        ```python
        # --- Main Execution Logic ---
def main():
    # ... (symbol loading)

    # Define backtest period (example: last 4 years)
    end_year = datetime.now().year
    start_year = end_year - 4

    # --- MODIFY HERE FOR TESTING --- #
    # Example: Run only for NIFTY for 2023
    symbols_to_run = ["NIFTY"]
    years_to_run = [2023]
    # symbols_to_run = all_symbols # Run for all symbols
    # years_to_run = range(start_year, end_year + 1)
    months_to_run = range(1, 13) # All months
    # --- END MODIFY --- #

    # ... (rest of the main function)
        ```
3.  **Execute:**
    *   Navigate to the directory containing `backtester_hedged.py`.
    *   Run the script using Python 3.11:
        ```bash
        python3.11 backtester_hedged.py
        ```
4.  **Output:**
    *   The script will log its progress to the console, including entry/exit details, hedging actions, and P&L for each month.
    *   Results are stored (or updated) in the `backtest_results` table in the database.

## Output Table (`backtest_results`)

This table stores the outcome of each monthly backtest attempt.

| Column Name         | Data Type        | Description                                                                           |
|---------------------|------------------|---------------------------------------------------------------------------------------|
| `symbol`            | TEXT             | Stock/Index symbol.                                                                   |
| `entry_date`        | DATE             | First trading day of the month when the trade was initiated (or attempted).           |
| `exit_date`         | DATE             | Date when the trade was closed (null if skipped or exit failed).                      |
| `year`              | INTEGER          | Year of the trade entry.                                                              |
| `month`             | INTEGER          | Month of the trade entry.                                                             |
| `entry_credit`      | DOUBLE PRECISION | Initial credit received (points) from selling the strangle (null if skipped).           |
| `exit_cost`         | DOUBLE PRECISION | Cost to close the strangle position (points) (null if skipped or exit failed).        |
| `pnl_points`        | DOUBLE PRECISION | **Total Profit or Loss** in points (Strangle P&L + Hedge P&L) (null if skipped).      |
| `skipped_reason`    | TEXT             | Reason for skipping trade (e.g., 'Earnings', 'No Options Found', etc.).               |
| `call_entry_strike` | DOUBLE PRECISION | Strike price of the Call option sold at entry.                                        |
| `put_entry_strike`  | DOUBLE PRECISION | Strike price of the Put option sold at entry.                                         |
| `call_entry_delta`  | DOUBLE PRECISION | Delta of the Call option at entry.                                                    |
| `put_entry_delta`   | DOUBLE PRECISION | Delta of the Put option at entry.                                                     |
| `call_entry_price`  | DOUBLE PRECISION | Price (settle) of the Call option at entry.                                           |
| `put_entry_price`   | DOUBLE PRECISION | Price (settle) of the Put option at entry.                                            |
| `call_exit_price`   | DOUBLE PRECISION | Price (settle) of the Call option at exit.                                            |
| `put_exit_price`    | DOUBLE PRECISION | Price (settle) of the Put option at exit.                                             |
| `hedge_pnl_points`  | DOUBLE PRECISION | Profit or Loss in points generated **only** from the weekly delta hedging activities. |

**Primary Key:** (`symbol`, `year`, `month`) - Ensures only one result per symbol/month (uses `ON CONFLICT DO UPDATE`).

## Assumptions & Limitations

*   **Data Granularity:** The backtest relies on daily data (`settle` prices for options, `underlying_price` for spot) from the `option_metrics` table. It does not use intraday data, so entry/exit/hedge prices are based on daily closing/settlement values.
*   **Hedging Instrument:** Hedging is performed using the **underlying spot asset**, not futures. P&L is based on the `underlying_price` changes.
*   **Transaction Costs:** Slippage, brokerage fees, taxes, and other transaction costs for both the strangle and the hedge trades are **not** factored into the P&L calculation.
*   **Data Availability & Quality:** The accuracy of the backtest depends heavily on the completeness and correctness of the data in the `option_metrics` table, including the `underlying_price` and the calculated deltas in the `option_chain` on all relevant dates (entry, exit, hedge dates).
*   **Lot Size:** P&L is calculated in points per single strangle (1 Call + 1 Put) and the corresponding hedge. To get actual monetary P&L, these points need to be multiplied by the appropriate contract multiplier (lot size), which is not included in this backtest.
*   **Working Days:** A 'working day' or 'trading day' is assumed to be any day for which a record exists in the `option_metrics` table for the given symbol.
*   **Delta Accuracy:** Assumes the `delta` values provided in the `option_chain` are accurate and suitable for hedging calculations.

