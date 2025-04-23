-- ===================================================
--  Table: option_metrics
--  Purpose: Store historical option data with IV, Greeks, and Volatility
-- ===================================================

CREATE TABLE IF NOT EXISTS option_metrics (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    underlying_price FLOAT,
    interest_rate FLOAT,
    strike_price FLOAT,
    expiry_30d DATE,
    expiry_60d DATE,
    expiry_90d DATE,
    upcoming_earning_date DATE,
    rv_yz FLOAT,
    ce JSONB,
    pe JSONB,
    option_chain JSONB,
    extras JSONB,
    PRIMARY KEY (symbol, date)
);

-- ===================================================
-- Indexes for performance optimization
-- ===================================================

-- Index to speed up symbol-date based queries
CREATE INDEX IF NOT EXISTS idx_symbol_date ON option_metrics(symbol, date);

-- Indexes on numeric fields used for filtering/sorting
CREATE INDEX IF NOT EXISTS idx_rv_yz ON option_metrics(rv_yz);
CREATE INDEX IF NOT EXISTS idx_strike_price ON option_metrics(strike_price);

-- JSONB Indexes
-- For filtering inside CE block (e.g., iv_30 or delta)
CREATE INDEX IF NOT EXISTS idx_ce_iv30 ON option_metrics ((ce->>'iv_30'));
CREATE INDEX IF NOT EXISTS idx_ce_delta ON option_metrics ((ce->'greeks'->>'delta'));

-- Full-text JSONB index on option_chain for array queries
CREATE INDEX IF NOT EXISTS idx_option_chain_gin ON option_metrics USING GIN (option_chain jsonb_path_ops);

-- Optionally index extras if it's queried often
-- CREATE INDEX IF NOT EXISTS idx_extras_field ON option_metrics ((extras->>'some_key'));
