-- SQL script to create the backtest_results table

CREATE TABLE IF NOT EXISTS backtest_results (
    symbol TEXT NOT NULL,
    entry_date DATE NOT NULL,
    exit_date DATE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    entry_credit DOUBLE PRECISION,
    exit_cost DOUBLE PRECISION,
    pnl_points DOUBLE PRECISION,
    skipped_reason TEXT,
    call_entry_strike DOUBLE PRECISION,
    put_entry_strike DOUBLE PRECISION,
    call_entry_delta DOUBLE PRECISION,
    put_entry_delta DOUBLE PRECISION,
    call_entry_price DOUBLE PRECISION,
    put_entry_price DOUBLE PRECISION,
    call_exit_price DOUBLE PRECISION,
    put_exit_price DOUBLE PRECISION,
    PRIMARY KEY (symbol, year, month)
);

-- Add indexes for faster querying
CREATE INDEX IF NOT EXISTS idx_backtest_results_symbol_date ON backtest_results (symbol, entry_date);
CREATE INDEX IF NOT EXISTS idx_backtest_results_pnl ON backtest_results (pnl_points);

ALTER TABLE backtest_results
ADD COLUMN IF NOT EXISTS hedge_pnl_points DOUBLE PRECISION;


