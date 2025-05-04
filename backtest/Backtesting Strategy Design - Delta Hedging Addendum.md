# Backtesting Strategy Design - Delta Hedging Addendum

This document details the design for incorporating weekly delta hedging into the existing strangle backtesting strategy, as requested by the user. It builds upon the original `strategy_design.md`.

## 1. Hedging Goal

To neutralize the directional risk (delta) of the short strangle position on a weekly basis by taking an offsetting position in the underlying asset.

## 2. Hedging Process within `run_backtest_month`

The hedging logic will be integrated into the main monthly backtest loop after the initial strangle entry and before the final exit.

*   **Initialization:** After the strangle entry on `entry_date`, initialize:
    *   `current_hedge_position` (float): Quantity of the underlying held for hedging. Starts at 0.
    *   `total_hedge_pnl` (float): Accumulated P&L from hedging activities. Starts at 0.
*   **Weekly Hedging Loop:**
    *   Identify all weekly hedging dates: Find the last trading day of each week (e.g., Friday, or Thursday if Friday is a holiday) that falls *after* the `entry_date` and *before* or on the `exit_date`.
    *   For each `hedge_date`:
        1.  **Fetch Data:** Get the `option_metrics` record for the `hedge_date` (need `underlying_price` and `option_chain`). If data is missing, skip hedging for this week.
        2.  **Calculate Strangle Delta:** Find the current delta of the originally sold Call (`call_entry_strike`, `expiry_30d`) and Put (`put_entry_strike`, `expiry_30d`) options using the `option_chain` from `hedge_date`. Handle cases where options might be missing (e.g., deep ITM/OTM with no data).
            *   `current_call_delta` = Delta of the short call position (usually negative).
            *   `current_put_delta` = Delta of the short put position (usually positive).
            *   `strangle_net_delta` = `current_call_delta` + `current_put_delta`. (Note: Deltas from `option_chain` are typically for long positions. Since we are short, the position delta is the negative of the chain delta, but convention varies. Let's assume the `black_scholes_greeks` function used in the ETL returns standard long deltas. So, for our short strangle: `strangle_net_delta = (-1 * call_delta_from_chain) + (-1 * put_delta_from_chain)`). **Correction:** The previous design doc used `Delta_Call - Delta_Put` assuming standard delta signs. Let's stick to that: `strangle_net_delta = call_delta_from_chain - put_delta_from_chain` (since short call is negative delta, short put is positive delta, the net delta calculation needs care. Let's re-verify: Short Call Delta = -Delta(Long Call), Short Put Delta = -Delta(Long Put). Portfolio Delta = Short Call Delta + Short Put Delta = -Delta(Long Call) - Delta(Long Put). If Delta(Long Call) is +0.2 and Delta(Long Put) is -0.2, Portfolio Delta = -0.2 - (-0.2) = 0. Okay, let's use `Portfolio Delta = - (Call Delta + Put Delta)` where Call/Put Delta are standard long deltas from Black-Scholes/option_chain).
        3.  **Calculate Total Portfolio Delta:**
            *   `hedge_position_delta` = `current_hedge_position` * 1 (Delta of underlying is 1).
            *   `total_portfolio_delta` = `strangle_net_delta` + `hedge_position_delta`.
        4.  **Determine Hedge Trade:**
            *   `required_hedge_adjustment` = `- total_portfolio_delta` (Amount of underlying to buy/sell to make the portfolio delta neutral).
        5.  **Simulate Hedge Trade:**
            *   If `required_hedge_adjustment` is non-zero:
                *   `hedge_trade_price` = `underlying_price` from `hedge_date` metrics.
                *   `cash_flow_hedge` = `- required_hedge_adjustment * hedge_trade_price` (Cash out if buying, cash in if selling).
                *   Update `total_hedge_pnl` += `cash_flow_hedge`.
                *   Update `current_hedge_position` += `required_hedge_adjustment`.
                *   Log the hedge action (e.g., "Hedged: Bought/Sold X units @ Price Y").
*   **Final Hedge Liquidation (on `exit_date`):**
    *   Fetch `underlying_price` for the `exit_date`.
    *   If `current_hedge_position` is non-zero:
        *   `liquidation_trade_quantity` = `- current_hedge_position` (Sell if holding long, buy if holding short).
        *   `liquidation_price` = `underlying_price` from `exit_date` metrics.
        *   `cash_flow_liquidation` = `- liquidation_trade_quantity * liquidation_price`.
        *   Update `total_hedge_pnl` += `cash_flow_liquidation`.
        *   Set `current_hedge_position` = 0.
        *   Log the liquidation.

## 3. P&L Calculation Update

*   Calculate `strangle_pnl_points` as before: `entry_credit - exit_cost`.
*   The `total_hedge_pnl` calculated above represents the P&L from hedging activities in points (assuming underlying price is in the same units as option prices).
*   The final `pnl_points` to be stored in the database is the sum:
    *   `final_pnl_points` = `strangle_pnl_points` + `total_hedge_pnl`.

## 4. Data Requirements & Assumptions

*   **Hedging Instrument:** Assumed to be the **underlying spot asset**. Hedging P&L is calculated based on changes in the `underlying_price` field from the `option_metrics` table.
*   **Futures Data:** No external futures data is required for this implementation.
*   **Option Deltas:** Assumes the `option_chain` in `option_metrics` contains accurate, standard (long position) delta values for the specific options on the hedging dates.
*   **Transaction Costs:** Hedge trade simulation does **not** include slippage or commissions.
*   **Hedging Threshold:** Assumes re-hedging to delta neutral (target delta = 0) every week if the portfolio delta is non-zero.

## 5. Database Table Update (`backtest_results`)

*   Add a new column to track the hedging P&L separately:
    *   `hedge_pnl_points` (DOUBLE PRECISION, nullable)
*   The existing `pnl_points` column will now store the **total combined P&L** (`strangle_pnl_points + hedge_pnl_points`).

## 6. Implementation Plan

1.  Modify `create_results_table.sql` to add the `hedge_pnl_points` column and potentially `ALTER` the existing table.
2.  Update `insert_backtest_result` function in `backtester.py` to accept and store `hedge_pnl_points`.
3.  Modify the main loop within `run_backtest_month` in `backtester.py` to:
    *   Initialize hedge variables.
    *   Implement the weekly hedging loop (find dates, fetch data, calculate deltas, simulate trades, update P&L and position).
    *   Implement final hedge liquidation on the exit date.
    *   Calculate the final combined `pnl_points`.
    *   Pass `hedge_pnl_points` and the combined `pnl_points` to `insert_backtest_result`.
4.  Test thoroughly (Step 011).
5.  Update documentation (Step 012).

