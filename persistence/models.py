CREATE_SESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS sessions (
    id                          SERIAL PRIMARY KEY,
    date                        DATE NOT NULL UNIQUE,
    starting_capital            NUMERIC(12,2) NOT NULL,
    ending_pnl                  NUMERIC(12,2) DEFAULT 0,
    trade_count                 INTEGER DEFAULT 0,
    win_count                   INTEGER DEFAULT 0,
    loss_count                  INTEGER DEFAULT 0,
    win_rate                    NUMERIC(5,2) DEFAULT 0,
    ivr_regime                  VARCHAR(10),
    circuit_breaker_triggered   BOOLEAN DEFAULT FALSE,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id                      SERIAL PRIMARY KEY,
    session_id              INTEGER REFERENCES sessions(id)
                            ON DELETE SET NULL,
    position_id             VARCHAR(20) UNIQUE,
    entry_timestamp         TIMESTAMPTZ NOT NULL,
    exit_timestamp          TIMESTAMPTZ,
    expiry_date             DATE NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    long_strike             INTEGER NOT NULL,
    short_strike            INTEGER NOT NULL,
    option_type             VARCHAR(2) NOT NULL,
    lots                    INTEGER NOT NULL,
    entry_premium_long      NUMERIC(10,2) NOT NULL,
    entry_premium_short     NUMERIC(10,2) NOT NULL,
    exit_premium_long       NUMERIC(10,2),
    exit_premium_short      NUMERIC(10,2),
    gross_pnl               NUMERIC(12,2),
    transaction_costs       NUMERIC(10,2),
    net_pnl                 NUMERIC(12,2),
    exit_reason             VARCHAR(30),
    created_at              TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_POSITIONS_TABLE = """
CREATE TABLE IF NOT EXISTS positions (
    id                      SERIAL PRIMARY KEY,
    trade_id                INTEGER REFERENCES trades(id)
                            ON DELETE CASCADE,
    position_id             VARCHAR(20),
    snapshot_timestamp      TIMESTAMPTZ NOT NULL,
    net_delta               NUMERIC(10,4),
    net_gamma               NUMERIC(10,6),
    net_theta               NUMERIC(10,4),
    net_vega                NUMERIC(10,4),
    iv_atm                  NUMERIC(8,4),
    mark_to_market          NUMERIC(12,2),
    created_at              TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_IV_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS iv_history (
    id                      SERIAL PRIMARY KEY,
    date                    DATE NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    iv_10am                 NUMERIC(8,4),
    iv_close                NUMERIC(8,4),
    ivr_computed            NUMERIC(8,4),
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(date, symbol)
);
"""

CREATE_RUNTIME_CONFIG_TABLE = """
CREATE TABLE IF NOT EXISTS runtime_config (
    id                      SERIAL PRIMARY KEY,
    config_date             DATE NOT NULL,
    symbol                  VARCHAR(20) NOT NULL,
    lot_size                INTEGER NOT NULL,
    strike_interval         INTEGER NOT NULL,
    expiry_type             VARCHAR(10) NOT NULL,
    next_expiry             DATE NOT NULL,
    expiry_weekday          VARCHAR(10) NOT NULL,
    ivr_credit_entry        NUMERIC(6,2) DEFAULT 50,
    ivr_credit_exit         NUMERIC(6,2) DEFAULT 45,
    ivr_debit_entry         NUMERIC(6,2) DEFAULT 30,
    ivr_debit_exit          NUMERIC(6,2) DEFAULT 35,
    pcr_bullish             NUMERIC(6,2) DEFAULT 1.35,
    pcr_bearish             NUMERIC(6,2) DEFAULT 0.75,
    atr_multiplier          NUMERIC(4,2) DEFAULT 2.0,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(config_date, symbol)
);
"""

# --- Indexes ---
# These are created separately from tables
# so they can be added to existing databases
# without recreating tables

CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_trades_session_id
    ON trades(session_id);

CREATE INDEX IF NOT EXISTS idx_trades_entry_timestamp
    ON trades(entry_timestamp);

CREATE INDEX IF NOT EXISTS idx_positions_trade_id
    ON positions(trade_id);

CREATE INDEX IF NOT EXISTS idx_positions_snapshot_timestamp
    ON positions(snapshot_timestamp);

CREATE INDEX IF NOT EXISTS idx_iv_history_date_symbol
    ON iv_history(date, symbol);

CREATE INDEX IF NOT EXISTS idx_iv_history_date
    ON iv_history(date DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_date
    ON sessions(date DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_config_date_symbol
    ON runtime_config(config_date DESC, symbol);
"""

ALL_TABLES = [
    CREATE_SESSIONS_TABLE,
    CREATE_TRADES_TABLE,
    CREATE_POSITIONS_TABLE,
    CREATE_IV_HISTORY_TABLE,
    CREATE_RUNTIME_CONFIG_TABLE,
]
