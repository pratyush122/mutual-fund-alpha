-- Migration 001: Create initial tables

-- Funds table
CREATE TABLE IF NOT EXISTS funds (
    scheme_code TEXT PRIMARY KEY,
    scheme_name TEXT NOT NULL,
    category TEXT,
    aum_cr REAL,  -- AUM in crores
    inception_date DATE
);

-- NAV history table
CREATE TABLE IF NOT EXISTS nav_history (
    id SERIAL PRIMARY KEY,
    scheme_code TEXT REFERENCES funds(scheme_code),
    date DATE NOT NULL,
    nav REAL NOT NULL,
    daily_return REAL,
    UNIQUE(scheme_code, date)
);

-- Factor data table
CREATE TABLE IF NOT EXISTS factor_data (
    date DATE PRIMARY KEY,
    mkt_rf REAL NOT NULL,
    smb REAL NOT NULL,
    hml REAL NOT NULL,
    rf REAL NOT NULL
);

-- Alpha results table
CREATE TABLE IF NOT EXISTS alpha_results (
    id SERIAL PRIMARY KEY,
    scheme_code TEXT REFERENCES funds(scheme_code),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    window_days INTEGER NOT NULL,
    alpha REAL,
    beta_mkt REAL,
    beta_smb REAL,
    beta_hml REAL,
    r_squared REAL,
    t_stat_alpha REAL,
    p_value REAL,
    sharpe REAL,
    info_ratio REAL,
    skill_score REAL,
    percentile_rank REAL,
    alpha_persistence REAL,  -- For ML readiness
    factor_exposures JSON,   -- For ML readiness
    rolling_windows JSON     -- For ML readiness
);