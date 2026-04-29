from datetime import date, time

# --- Execution Windows ---
WINDOW_1_OPEN = time(9, 30)
WINDOW_1_CLOSE = time(13, 0)
WINDOW_2_OPEN = time(13, 30)
WINDOW_2_CLOSE = time(14, 30)
HARD_KILL_TIME = time(15, 10)
PCR_ACTIVATION_TIME = time(9, 30)
NEAR_EXPIRY_EXIT_TIME = time(13, 30)
PREMARKET_INIT_TIME = time(8, 45)
WEBSOCKET_OPEN_TIME = time(9, 15)

# --- Signal Confirmation ---
VWAP_CONFIRMATION_CLOSES = 1
VWAP_CONFIRMATION_WINDOW = 3  # minutes
VWAP_PULLBACK_TOLERANCE = 0.0045  # 0.45% max distance from VWAP for entry

# --- Exit Rules ---
THETA_STAGNATION_MINUTES = 45
THETA_MINIMUM_GAIN_PCT = 0.15  # 15% favourable move required
PROFIT_TARGET = 0.80  # 100% Theta Harvester — captures full credit
THETA_DAYS = 5  # Multi-day holding period

# --- Risk (Human Decisions) ---
MAX_CONCURRENT_POSITIONS = 4  # 2 Iron Condors × 2 legs (CREDIT_PUT + CREDIT_CALL)
RISK_PER_TRADE_PCT = 0.70  # 90% of account per trade
CIRCUIT_BREAKER_PCT = 0.10  # 10% daily drawdown limit
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.0  # Updated after backtest calibration
SPREAD_WIDTH = 6


# --- VIX Range Filter (matches backtest_engine.py exactly) ---
# run_true_chronological.py: min_vix=11.0, max_vix=22.0
VIX_MIN = 11.0
VIX_MAX = 22.0

# --- PCR Thresholds ---
# These are starting values — update after backtest Phase 1B calibration
PCR_BULLISH = 1.35
PCR_BEARISH = 0.75

# --- WebSocket ---
HEARTBEAT_INTERVAL = 15  # seconds
HEARTBEAT_TIMEOUT = 5  # seconds
RECONNECT_MAX_WAIT = 30  # seconds
STALE_FEED_THRESHOLD = 30  # seconds

# --- Analytics ---
IVR_UPDATE_INTERVAL = 300  # seconds (5 minutes)
OI_SNAPSHOT_INTERVAL = 60  # seconds
OI_WALL_UPDATE_INTERVAL = 300  # seconds
GREEKS_BATCH_INTERVAL = 1  # second
ATM_STRIKE_RANGE = 5  # ATM ± 5 strikes

# --- PCR Signal ---
PCR_SIGNAL_VALIDITY = 30  # minutes

# --- Backtest Slippage ---
BACKTEST_SLIPPAGE_FLAT = 0.50  # minimum ₹ per leg
BACKTEST_SLIPPAGE_PCT = 0.005  # 0.5% of close price

# --- Transaction Costs ---
# STT on shorted options: 0.15% of premium
# Effective April 2025 per SEBI circular
# Note: Exercised options STT does not apply —
# system exits all positions before expiry
STT_RATE = 0.0015

# SEBI turnover charge — verified March 2026
SEBI_CHARGE_RATE = 0.000001

# NSE equity options transaction charge
# ₹35.53 per lakh = 0.035530% — verified March 2026
NSE_TRANSACTION_RATE = 0.0003553

# Dhan flat brokerage per order leg
# Verify against your specific Dhan account agreement
BROKERAGE_PER_LEG = 20.0

# GST on brokerage — verified March 2026
GST_RATE = 0.18

# --- Risk-Free Rate ---
# RBI 91-day T-bill cut-off yield
# Auction date: March 18 2026, Yield: 5.3278%
# Source: RBI press release
# Update after every Tuesday RBI T-bill auction:
# rbi.org.in → Press Releases → Treasury Bill Auction Results
DEFAULT_RISK_FREE_RATE = 0.0521

# --- Macro Events ---
MACRO_BLACKOUT_PRE = 5  # minutes before Tier-1 event
MACRO_BLACKOUT_POST = 15  # minutes after Tier-1 event

# --- Alerts ---
WATCHDOG_INTERVAL = 30  # seconds
WATCHDOG_MISS_THRESHOLD = 2  # consecutive misses before alert

# --- Asyncio Queues ---
OPTIONS_QUEUE_SIZE = 512
FUTURES_QUEUE_SIZE = 512
SIGNAL_QUEUE_SIZE = 64

# --- WebSocket Behaviour ---
WS_SUBSCRIPTION_CHUNK_SIZE = 50  # Max tokens per subscription message
WS_RECONNECT_DELAYS = [1, 2, 4, 8, 16, 30]  # seconds, exp backoff

# --- Rate Verification ---
# Transaction cost rates — STT, SEBI, NSE, brokerage, GST
# These cannot be fetched programmatically.
# Verify manually at:
#   NSE circulars  → nseindia.com/regulations/circulars
#   SEBI circulars → sebi.gov.in/legal/circulars
#
# Update RATE_LAST_VERIFIED_DATE every time you manually
# verify any transaction cost rate above.
RATE_LAST_VERIFIED_DATE = date(2026, 3, 21)
RATE_WARN_DAYS = 180
RATE_HIGH_DAYS = 270
RATE_CRITICAL_DAYS = 365

# Risk-free rate — tracked separately from transaction costs
# because it changes weekly (every Tuesday RBI auction)
# while transaction cost rates change only via circulars.
# Update RFR_LAST_UPDATED_DATE after every rate update.
RFR_LAST_UPDATED_DATE = date(2026, 4, 27)
RFR_STALE_DAYS = 10
