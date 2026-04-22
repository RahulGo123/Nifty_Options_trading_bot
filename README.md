# Multi-Factor Volatility Regime Trading Bot

### V4.1 — Production Architecture | NSE Index F&O | Nifty 50 Weekly Options

---

## What's New in V4.1 (Resilience & Reporting Update)

- **Dynamic Strike Rounding**: Computes ATM strikes by dynamically pulling the `strike_interval` from the Scrip Master, preventing failures when NSE shifts to 100-point strikes.
- **Reporting Enhancements**: EOD Telegram reports now correctly track "New entries" and accurately report overnight Carry positions instead of mislabeling active days as "No trades". Includes accurate session-level tracking of win rates and loss counts.
- **EOD Safety Net**: The 03:30 PM shutdown sequence now acts as a secondary safety net, actively guaranteeing report delivery even if the 03:10 PM primary dispatch encounters a failure.
- **Session Upserts**: `write_buffer.py` utilizes `ON CONFLICT DO UPDATE` clauses to enable live updates of P&L metadata and trade states, preventing Postgres unique key collisions.
- **Expanded Strike Fetch**: Amplified `atm_range` to ±60 strikes at the 08:45 AM Scrip Master fetch to prevent the Basket Builder from crashing during extreme intraday spot moves.
- **Alert Spam Prevention**: Limits missing Scrip Master token alerts to a single Telegram message per session to keep logs clean and reduce noise.
- **Ghost Mode Reconciliation**: Prevents false-positive circuit breakers by bypassing broker integration checks while `LIVE_TRADING=False`.
- **Hot-Boot Session Recovery**: Real-time write tracking to `capital_state.json` ensures that both floating positions and realized P&L persist natively across unforeseen process restarts.

---

## Instrument Universe (SEBI-Compliant, Post November 20 2024)

| Index    | Type                 | Lot Size | Strike Interval | Status           |
| -------- | -------------------- | -------- | --------------- | ---------------- |
| Nifty 50 | Weekly Options (NSE) | 65       | 50 points       | Phase 1 — Active |

> **SEBI Circular (Nov 20 2024):** NSE is permitted to offer weekly expiry F&O contracts for only one benchmark index. That index is Nifty 50. All other weekly contracts (BankNifty, FinNifty, MidcapNifty) are discontinued.

---

## Capital & Risk Parameters

| Parameter                | Value                         |
| ------------------------ | ----------------------------- |
| Total Deployed Capital   | Dynamic (Baseline ₹1,00,000)  |
| Max Risk Per Trade       | 90% of Total Capital          |
| Daily Circuit Breaker    | 10% daily drawdown limit      |
| Spread Structure         | Iron Condor (4 Legs, Width 6) |
| Max Concurrent Positions | 4 spreads                     |
| Stop Loss Distance       | 2.0 × ATR-14                  |

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Setup](#2-repository-setup)
3. [Environment Configuration](#3-environment-configuration)
4. [Database Setup](#4-database-setup)
5. [Data Acquisition](#5-data-acquisition)
6. [Backtesting Harness](#6-backtesting-harness)
7. [In-Sample Calibration](#7-in-sample-calibration)
8. [Out-of-Sample Blind Validation](#8-out-of-sample-blind-validation)
9. [Ghost Mode Deployment](#9-ghost-mode-deployment)
10. [Live Capital Deployment](#10-live-capital-deployment)
11. [Daily Operational Timeline](#11-daily-operational-timeline)
12. [Project Directory Structure](#12-project-directory-structure)
13. [Module Reference](#13-module-reference)
14. [Monitoring & Alerts](#14-monitoring--alerts)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Prerequisites

### 1.1 Accounts & Credentials Required

Before writing a single line of code, obtain all of the following. The system cannot be built without them.

| Requirement              | Purpose                             | Where to Get                           |
| ------------------------ | ----------------------------------- | -------------------------------------- |
| Dhan Trading Account     | Live order execution                | Dhanoswalsecurities.com                |
| Dhan API Access          | WebSocket + REST API                | Apply via MO developer portal          |
| Dhan TOTP Secret         | Programmatic 2FA login              | Enable in MO account security settings |
| Neon PostgreSQL Database | Trade logging, IV history, sessions | neon.tech (free tier sufficient)       |
| Telegram Bot Token       | Alerts and EOD reports              | @BotFather on Telegram                 |
| Telegram Chat ID         | Your personal alert channel         | @userinfobot on Telegram               |

### 1.2 System Requirements

| Component | Minimum                   | Recommended                      |
| --------- | ------------------------- | -------------------------------- |
| Python    | 3.10+                     | 3.11                             |
| RAM       | 8 GB                      | 16 GB                            |
| Storage   | 10 GB free                | 20 GB free                       |
| OS        | Ubuntu 20.04 / Windows 10 | Ubuntu 22.04                     |
| Network   | Stable broadband          | Dedicated low-latency connection |

### 1.3 Production Deployment (Cloud — Recommended)

For fully autonomous operation without manual startup every morning, deploy on a cloud VM. The scheduler will trigger the bot at 08:45 AM IST automatically every trading day.

| Provider              | Recommended Instance             | Estimated Cost |
| --------------------- | -------------------------------- | -------------- |
| Google Cloud Platform | e2-standard-2 (2 vCPU, 8 GB RAM) | ~₹2,500/month  |
| AWS                   | t3.small (2 vCPU, 2 GB RAM)      | ~₹1,800/month  |
| Azure                 | B2s (2 vCPU, 4 GB RAM)           | ~₹2,200/month  |

> Set the VM timezone to **Asia/Kolkata (IST)** immediately after provisioning. Every scheduled job in the system depends on this.

### 1.4 Python Libraries

```
asyncio
asyncpg
pyotp
numpy
scipy
pandas
pyarrow          # Parquet support
fastparquet      # Parquet alternative
aiohttp
websockets
python-decouple
APScheduler
requests
loguru
telegram         # python-telegram-bot
multiprocessing  # stdlib
```

---

## 2. Repository Setup

### 2.1 Clone and Create Directory Structure

Create the following directory structure exactly. Every module reference in this document maps to a specific file path.

```
trading_bot/
├── backtest/
│   ├── data_pipeline.py
│   ├── data_cleaner.py
│   ├── backtest_engine.py
│   ├── slippage_model.py
│   └── results_analyzer.py
├── config/
│   ├── settings.py
│   ├── constants.py
│   └── contracts.py
├── auth/
│   └── Dhan_auth.py
├── data/
│   ├── scrip_master.py
│   ├── websocket_manager.py
│   ├── tick_dispatcher.py
│   └── oi_tracker.py
├── analytics/
│   ├── worker_process.py
│   ├── greeks_engine.py
│   ├── ivr_engine.py
│   ├── atr_engine.py
│   ├── vwap_engine.py
│   ├── pcr_engine.py
│   ├── oi_wall_scanner.py
│   └── cost_model.py
├── signals/
│   ├── regime_gate.py
│   ├── bias_gate.py
│   └── trigger_gate.py
├── risk/
│   ├── portfolio_state.py
│   ├── position_sizer.py
│   ├── circuit_breaker.py
│   └── pre_execution.py
├── orders/
│   ├── basket_builder.py
│   ├── order_manager.py
│   └── ghost_ledger.py
├── positions/
│   ├── lifecycle_manager.py
│   └── greeks_monitor.py
├── persistence/
│   ├── write_buffer.py
│   ├── database.py
│   └── models.py
├── monitoring/
│   ├── alerting.py
│   ├── health_check.py
│   └── reporter.py
├── scheduler/
│   └── market_scheduler.py
├── macro/
│   └── event_calendar.py
├── data_store/
│   ├── raw_csv/             # TradingTuitions raw CSV files (temporary)
│   └── parquet/             # Cleaned Parquet files (permanent)
├── main.py
├── .env
├── .env.example
└── requirements.txt
```

### 2.2 Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Environment Configuration

### 3.1 Create the `.env` File

Copy `.env.example` to `.env` and fill in every value. Never commit `.env` to version control.

```env
# --- Execution Mode ---
LIVE_TRADING=False

# --- Dhan API ---
MO_CLIENT_ID=your_client_id
MO_PASSWORD=your_password
MO_TOTP_SECRET=your_totp_secret_key
MO_API_KEY=your_api_key

# --- Database ---
DATABASE_URL=postgresql://user:password@host/dbname

# --- Telegram ---
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# --- Capital ---
TOTAL_CAPITAL=500000
MAX_RISK_PER_TRADE=10000
DAILY_CIRCUIT_BREAKER=30000
MAX_CONCURRENT_POSITIONS=3
```

### 3.2 Verify `constants.py`

Confirm the following values are set correctly in `config/constants.py` before any backtest or live run.

```
# Instrument
NIFTY_LOT_SIZE = 65
NIFTY_STRIKE_INTERVAL = 50

# Strategy & Risk Multipliers
SPREAD_WIDTH = 6
RISK_PER_TRADE_PCT = 0.90
CIRCUIT_BREAKER_PCT = 0.10

# VIX Range Filter
VIX_MIN = 11.0
VIX_MAX = 22.0

# IVR Thresholds
IVR_CREDIT_ENTRY = 50
IVR_CREDIT_EXIT = 45
IVR_DEBIT_ENTRY = 30
IVR_DEBIT_EXIT = 35

# PCR Thresholds
PCR_BULLISH = 1.35
PCR_BEARISH = 0.75

# ATR
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.0             # Calibrated in backtest — update after Phase 1B

# VWAP
VWAP_CONFIRMATION_CLOSES = 1
VWAP_CONFIRMATION_WINDOW_MINUTES = 3
VWAP_PULLBACK_TOLERANCE = 0.0045

# Exits
THETA_STAGNATION_MINUTES = 45
THETA_MINIMUM_GAIN_PCT = 0.15
PROFIT_TARGET = 1.0              # 100% Theta Harvester
THETA_DAYS = 5                   # Multi-day holding period

# Execution Windows
WINDOW_1_OPEN = "09:30"
WINDOW_1_CLOSE = "13:00"
WINDOW_2_OPEN = "13:30"
WINDOW_2_CLOSE = "14:30"
HARD_KILL_TIME = "15:10"
PCR_ACTIVATION_TIME = "09:30"
```

---

## 4. Database Setup

### 4.1 Create the Four Core Tables

Connect to your Neon PostgreSQL database and run the schema creation. The four tables are:

**`sessions`** — One record per trading day.
Fields: `id`, `date`, `starting_capital`, `ending_pnl`, `trade_count`, `win_rate`, `ivr_regime`, `circuit_breaker_triggered`, `created_at`

**`trades`** — One record per filled basket order.
Fields: `id`, `session_id`, `entry_timestamp`, `exit_timestamp`, `expiry_date`, `long_strike`, `short_strike`, `option_type` (CE/PE), `entry_premium_long`, `entry_premium_short`, `exit_premium_long`, `exit_premium_short`, `gross_pnl`, `transaction_costs`, `net_pnl`, `exit_reason`, `lots`

**`positions`** — Greeks snapshots every 5 minutes for open positions.
Fields: `id`, `trade_id`, `snapshot_timestamp`, `net_delta`, `net_theta`, `net_vega`, `iv_atm`, `mark_to_market`

**`iv_history`** — One record per trading day for IVR bootstrap.
Fields: `id`, `date`, `symbol`, `iv_10am`, `iv_close`, `ivr_computed`

### 4.2 Verify Database Connection

Run the database connection test from `persistence/database.py` before proceeding. If this fails, no other layer will function correctly.

---

## 5. Data Acquisition

This step is completed once before any backtesting begins. Do not skip it.

### 5.1 Download TradingTuitions 1-Minute Intraday Data

**What you are downloading:** 1-minute OHLCV + Open Interest for Nifty 50 options (weekly and monthly), Nifty 50 Futures, and Nifty 50 spot index. Coverage: January 2021 to the data pause date.

**Steps:**

1. Visit the TradingTuitions website and locate the intraday options data section.
2. Register with your email address to receive the Google Drive access link.
3. Download all files tagged as `NIFTY_WEEKLY` options data.
4. Download the Nifty 50 Futures 1-minute data separately.
5. Place all raw CSV files into `data_store/raw_csv/`.

**Why this source:** It is the only free source that provides 1-minute OI snapshots alongside OHLCV. This is mandatory for calculating the 10:30 AM PCR without look-ahead bias and for tracking the intraday Max OI wall shift. Bhavcopy (NSE EOD data) cannot substitute for this — EOD OI introduces look-ahead bias into every PCR calculation.

### 5.2 Download NSE Bhavcopy for IV History Bootstrap

The `iv_history` table requires 252 trading days of historical ATM IV. This comes from NSE's official FnO Bhavcopy archive, not from TradingTuitions.

**Install jugaad-data:**

```bash
pip install jugaad-data
```

**Download FnO Bhavcopy (2021 to present):**
Use `jugaad-data` to loop over all trading days and download the daily FnO bhavcopy CSV for each day. Add a 2–3 second random delay between each request to avoid NSE IP blocking. Store each file in `data_store/raw_csv/bhavcopy/`.

**Parse and load into `iv_history`:**
For each bhavcopy file, filter for `Symbol=NIFTY` and `Instrument=OPTIDX`. Identify the ATM strike (nearest to Nifty spot close). Extract the settlement price for the ATM CE and ATM PE. Run the BSM solver from `analytics/greeks_engine.py` on each to compute implied IV. Write one row per day to the `iv_history` table.

**Total download time:** 3–5 hours with throttled requests.
**Total storage for raw bhavcopy:** ~2–4 GB.
**Storage after filtering for Nifty only:** Under 200 MB.

---

## 6. Backtesting Harness

This entire phase runs offline, before any live system component is touched.

### 6.1 Ingest Raw CSVs into Parquet

Run `backtest/data_pipeline.py` to convert all TradingTuitions raw CSVs into Parquet columnar format.

**Why Parquet:** Three years of 1-minute options data loaded directly into pandas from raw CSV will exhaust 16 GB of RAM. Parquet enables millisecond-speed time-slice queries on specific dates, strikes, and times without loading the full dataset into memory.

**Output:** All cleaned data written to `data_store/parquet/`.

### 6.2 Run the Data Cleaner

Run `backtest/data_cleaner.py` on all Parquet files before any strategy logic touches them.

**Cleaning rules applied:**

- Price gaps under 5 consecutive minutes: forward-fill price, zero-fill volume.
- Price gaps of 5 or more consecutive minutes: flag the entire surrounding signal window as excluded. Do not fill — a 5-minute gap means OI and PCR data for that window is unreliable.
- Any trading day with more than 30 minutes of total flagged gaps: exclude the entire day from the backtest dataset. Log the excluded dates to a file for review.

**Output:** A `data_quality_report.csv` listing every gap detected, its duration, and the action taken (filled or excluded).

### 6.3 Verify Data Integrity Before Proceeding

Before running a single backtest iteration, confirm the following manually:

- [ ] Total trading days in dataset matches expected count for 2021 to present.
- [ ] No single excluded day accounts for a major market event (check excluded dates against NSE holiday calendar).
- [ ] The 10:30 AM candle exists for at least 95% of trading days in the dataset.
- [ ] OI values are non-zero for ATM ± 5 strikes on at least 95% of trading days.

If any check fails, review the raw CSV files for that period before proceeding.

---

## 7. In-Sample Calibration (2021 — Mid 2023)

### 7.1 What Is Being Calibrated

The backtest engine does not validate whether the strategy makes money. It calibrates the exact parameter values that maximise risk-adjusted returns. The parameters being optimised are:

| Parameter                  | Starting Value | Range to Test |
| -------------------------- | -------------- | ------------- |
| IVR Credit Entry Threshold | 50             | 45 – 60       |
| IVR Debit Entry Threshold  | 30             | 20 – 35       |
| PCR Bullish Threshold      | 1.35           | 1.20 – 1.60   |
| PCR Bearish Threshold      | 0.75           | 0.60 – 0.90   |
| ATR Multiplier             | 2.0            | 1.5 – 3.0     |
| VWAP Confirmation Closes   | 1              | 1 – 2         |
| Theta Stagnation Window    | 45 min         | 30 – 90 min   |

### 7.2 Walk-Forward Optimisation Protocol

Do not optimise across the entire 2021 to mid-2023 dataset in one pass. This produces overfit parameters. Instead, use rolling 6-month windows:

- Window 1: Train on Jan 2021 – Jun 2021, validate on Jul 2021 – Dec 2021
- Window 2: Train on Jul 2021 – Dec 2021, validate on Jan 2022 – Jun 2022
- Window 3: Train on Jan 2022 – Jun 2022, validate on Jul 2022 – Dec 2022
- Window 4: Train on Jul 2022 – Dec 2022, validate on Jan 2023 – Jun 2023

**Optimisation objective:** Sharpe ratio. Not raw P&L. Not win rate. Sharpe ratio accounts for both returns and volatility of returns.

**Output:** A parameter set that is stable across rolling windows. If the optimal ATR multiplier is 1.5 in Window 1 and 2.2 in Window 4, the parameters are regime-dependent and not robust. Investigate before proceeding.

### 7.3 Update `constants.py` With Calibrated Values

After walk-forward optimisation completes, update `config/constants.py` with the calibrated parameter values. Document the calibration date and the Sharpe ratio achieved on each validation window.

---

## 8. Out-of-Sample Blind Validation (Mid 2023 — 2024)

### 8.1 The Rules of This Phase

This phase has exactly one rule: **run once, change nothing.**

The out-of-sample dataset (mid 2023 to 2024) has not been touched during calibration. It represents genuine unseen data. Run the backtest engine with the calibrated parameters from Phase 7 against this dataset exactly once.

**Do not:**

- Adjust any threshold after seeing the results.
- Re-run with different parameters to improve the numbers.
- Exclude any periods from the out-of-sample dataset.

### 8.2 Pass / Fail Criteria

| Metric                                   | Pass                                     | Fail                                 |
| ---------------------------------------- | ---------------------------------------- | ------------------------------------ |
| Out-of-sample Sharpe vs in-sample Sharpe | Degrades less than 40%                   | Degrades 40% or more                 |
| Max drawdown                             | Within 1.5× of in-sample max drawdown    | Exceeds 2× in-sample max drawdown    |
| Win rate                                 | Within 10 percentage points of in-sample | Drops more than 15 percentage points |

**If all three pass:** The model is validated. Proceed to Phase 2 (Ghost Mode).

**If any metric fails:** The signal logic has a structural flaw. Do not re-tune thresholds. Revisit the gate logic in `signals/`. Common causes: Gate 2 PCR calculation is using incorrect OI window, Gate 3 VWAP confirmation is too loose, or the IVR hysteresis bands are creating regime persistence that does not hold in trending markets.

---

## 9. Ghost Mode Deployment

### 9.1 Set `LIVE_TRADING = False`

Confirm in `.env`:

```
LIVE_TRADING=False
```

In Ghost Mode, the system runs the complete live pipeline — WebSocket, signal engine, risk engine, order management — but all fills are simulated by the `VirtualPortfolio` in `orders/ghost_ledger.py`.

**Ghost Ledger fill rules (strictly enforced):**

- Buys fill at live WebSocket **Ask price** + slippage buffer
- Sells fill at live WebSocket **Bid price** − slippage buffer
- Exact transaction costs (STT, SEBI, brokerage, GST) are simulated natively
- Overnight Carry and 100% Theta Harvest simulated equivalently to real trading
- Portfolio dynamically scales capital requirements on live margins

### 9.2 Minimum Ghost Mode Duration

Run Ghost Mode for a minimum of **2 weeks** before any live capital is deployed. Two weeks is the minimum — longer is better.

Ghost Mode is not validating signal logic. That is already done in the backtest. Ghost Mode is validating:

- Dhan API latency and reliability
- Real Bid/Ask slippage vs the synthetic backtest slippage model
- WebSocket reconnection behaviour during real market sessions
- APScheduler job accuracy (does 08:45 AM init actually fire at 08:45 AM IST?)
- Telegram alert delivery
- Database write buffer reliability under load
- Health watchdog subprocess stability

### 9.3 Ghost Mode Acceptance Criteria

Before proceeding to live deployment, all of the following must be confirmed:

- [ ] Zero WebSocket disconnections lasting more than 30 seconds during market hours over the 2-week period.
- [ ] All APScheduler jobs fire within 30 seconds of their scheduled time.
- [ ] The 03:10 PM hard kill executes without manual intervention on every session.
- [ ] Telegram EOD report received every trading day.
- [ ] No missed stop-loss ticks — verify by comparing `portfolio_state.py` timestamps against database snapshots.
- [ ] Ghost Ledger P&L is in line with backtest expectations (within 30% degradation due to real slippage vs synthetic).
- [ ] Zero database write failures logged over the 2-week period.
- [ ] Health watchdog fired zero false alerts.

If any criterion fails, identify and fix the infrastructure issue before proceeding. Do not move to live capital until all criteria pass.

---

## 10. Live Capital Deployment

### 10.1 The Only Change Required

Change one line in `.env`:

```
LIVE_TRADING=True
```

Nothing else changes. The same codebase, the same parameters, the same risk limits. The `LIVE_TRADING` singleton propagates through every layer automatically.

### 10.2 Live Mode Behaviour Changes

| Component         | Ghost Mode                          | Live Mode                             |
| ----------------- | ----------------------------------- | ------------------------------------- |
| Order execution   | `VirtualPortfolio` fills at Bid/Ask | Dhan basket order API                 |
| Margin check      | Simulated SPAN formula              | Live broker margin query              |
| Fill confirmation | Instant (simulated)                 | Awaits broker order confirmation      |
| Circuit breaker   | Closes virtual positions            | Sends market-order square-off via API |

### 10.3 First Week of Live Trading

- Monitor every session manually for the first 5 trading days.
- Keep the Telegram alert channel open on your phone at all times.
- Do not modify any parameters during the first week regardless of daily P&L.
- Review the EOD report every evening and compare actual fills against Ghost Ledger fills from the same session.

### 10.4 Ongoing Capital Management

The system manages ₹5,00,000 with a 2% per-trade risk rule and a 6% daily circuit breaker. After the first 30 trading days of live operation, review performance against the backtest Sharpe ratio. If live Sharpe is within 40% of backtest Sharpe, the system is operating as designed. If degradation exceeds 40%, pause live trading and re-examine slippage assumptions before continuing.

---

## 11. Daily Operational Timeline

The system runs autonomously on every NSE trading day. No manual intervention is required after the initial deployment.

| Time (IST) | Event                                            | Module                           |
| ---------- | ------------------------------------------------ | -------------------------------- |
| 08:45 AM   | TOTP authentication, session token generation    | `auth/Dhan_auth.py`              |
| 08:45 AM   | Scrip Master CSV download and token map build    | `data/scrip_master.py`           |
| 08:45 AM   | IV history bootstrap check (252-day IVR seed)    | `analytics/ivr_engine.py`        |
| 08:45 AM   | Macro calendar fetch, Tier-1 event scheduling    | `macro/event_calendar.py`        |
| 09:15 AM   | WebSocket connection opened, subscriptions sent  | `data/websocket_manager.py`      |
| 09:15 AM   | Greeks worker process spawned                    | `analytics/worker_process.py`    |
| 09:15 AM   | VWAP engine reset and running-sum initialised    | `analytics/vwap_engine.py`       |
| 09:30 AM   | Execution Window 1 opens — new entries permitted | `risk/pre_execution.py`          |
| 09:30 AM   | PCR engine activates (Gate 2 becomes evaluable)  | `signals/bias_gate.py`           |
| 10:00 AM   | First IVR evaluation (Gate 1 becomes active)     | `signals/regime_gate.py`         |
| 01:00 PM   | Execution Window 1 closes — no new entries       | `risk/pre_execution.py`          |
| 01:30 PM   | Execution Window 2 opens                         | `risk/pre_execution.py`          |
| 02:30 PM   | Execution Window 2 closes — no new entries       | `risk/pre_execution.py`          |
| 03:10 PM   | Hard kill — all positions squared off            | `positions/lifecycle_manager.py` |
| 03:10 PM   | WebSocket connection severed                     | `data/websocket_manager.py`      |
| 03:10 PM   | Greeks worker process terminated                 | `analytics/worker_process.py`    |
| 03:10 PM   | EOD P&L report dispatched to Telegram            | `monitoring/reporter.py`         |
| 03:10 PM   | 20-day SMA updated with today's Futures close    | `analytics/pcr_engine.py`        |
| 03:15 PM   | Session record written to `sessions` table       | `persistence/database.py`        |

---

## 12. Project Directory Structure

```
trading_bot/
├── analytics/
│   ├── worker_process.py        # multiprocessing.Process entry point
│   ├── theta_tracker.py         # Multi-day 100% Theta Harvester
│   ├── greeks_engine.py         # Vectorized BSM, Newton-Raphson array solver
│   ├── ivr_engine.py            # IVR-252 with hysteresis, 5-min refresh, bootstrap
│   ├── atr_engine.py            # ATR-14 on 15-min Futures candles
│   ├── vwap_engine.py           # Cumulative Futures VWAP, O(1) running-sum update
│   ├── pcr_engine.py            # ΔOI PCR engine and signal overrides
│   ├── oi_wall_scanner.py       # Max OI strike finder (CE above ATM, PE below ATM)
│   ├── cost_model.py            # STT + SEBI + NSE charges + brokerage + GST
│   ├── gap_tracker.py           # Anomalous tick/data gap detection
│   └── rate_validator.py        # Validates rate-limit boundaries from broker API
├── auth/
│   └── Dhan_auth.py             # pyotp TOTP, session token, proactive refresh
├── backtest/
│   ├── backtest_engine.py       # Core loop for processing events sequentially
│   ├── data_cleaner.py          # Forward-fill, zero-fill, day exclusion logic
│   ├── data_pipeline.py         # CSV → Parquet ingestion pipeline
│   ├── slippage_model.py        # Synthetic live slippage & order crossing
│   ├── results_analyzer.py      # EOD Metric analysis (Sharpe, Drawdowns)
│   ├── run_calibration.py       # Out-of-sample parametric range optimization
│   ├── run_true_chronological.py# Core unified testing execution harness
│   └── dhan_data_fetcher.py     # Pulls historical market archives
├── config/
│   ├── settings.py              # Settings dataclass, env var loader
│   ├── constants.py             # Global constants, widths, ATR specs
│   └── contracts.py             # Financial instrument metadata and lots
├── data/
│   ├── scrip_master.py          # CSV fetch, core strike and token map tracking
│   ├── websocket_manager.py     # Async WS routing, exponential backoffs
│   ├── tick_dispatcher.py       # Routes inbound signals to correct queues
│   ├── oi_tracker.py            # Generates rolling snapshot vectors
│   ├── market_data_store.py     # High-speed unified dictionary for latest ticks
│   ├── runtime_config.py        # Synchronized access to live API rules
│   └── ghost_snapshot.json      # Inter-session simulated snapshot state
├── execution/
│   └── margin_cache.py          # Advanced SPAN Margin estimation tables
├── macro/
│   └── event_calendar.py        # Tier-1/2 scheduled data releases
├── monitoring/
│   ├── alerting.py              # Central Telegram alert queue structure
│   ├── health_check.py          # Independent subsystem ping loop
│   ├── reporter.py              # Final daily P&L session compilation
│   └── telegram_listener.py     # Listens to manual live-chat overrides
├── orders/
│   ├── basket_builder.py        # Assembles N-Leg arrays from core strategies
│   ├── ghost_ledger.py          # Core virtual portfolio executor (Fills + Costs)
│   └── order_manager.py         # Synchronizes real orders to broker API
├── persistence/
│   ├── write_buffer.py          # Batches inserts with PostgreSQL ON CONFLICT updates
│   ├── database.py              # asyncpg multi-pool connection interface
│   └── models.py                # SQL database table representations
├── positions/
│   ├── greeks_monitor.py        # Pings position portfolio for greek volatility
│   └── lifecycle_manager.py     # Forces EOD closure, stops, expirations
├── risk/
│   ├── circuit_breaker.py       # System lockdown enforcing daily limits
│   ├── portfolio_state.py       # Floating P&L, Active Leg states, Risk margin limits
│   ├── position_sizer.py        # Volatility scaled portfolio exposure calculation
│   ├── pre_execution.py         # Enforces structural entry execution windows
│   ├── correlation.py           # Detects excess multi-index leverage
│   └── reconciliation.py        # System logic checking mismatch against real broker
├── scheduler/
│   └── market_scheduler.py      # Trigger hooks for EOD, pre-market, API handshakes
├── data_store/                  # Cleaned & Temporary datasets
├── main.py                      # Orchestrator, process spawner, event loop
├── manual_report_today.py       # Override dispatch for standalone reports
├── requirements.txt             # Primary application dependencies
└── .env                         # Secrets (Ignored by VCS)
```

---

## 13. Module Reference

### 13.1 Signal Engine — Three-Gate Architecture

All three gates must pass sequentially. Failure at any gate resets the full evaluation.

```
Gate 1 (IVR Regime)  →  Gate 2 (PCR Bias)  →  Gate 3 (VWAP Trigger)  →  Risk Engine
```

**Gate 1 — Macro Regime (`signals/regime_gate.py`)**

| Condition                   | Regime      | Allowed Strategy                   |
| --------------------------- | ----------- | ---------------------------------- |
| IVR > 50                    | Credit      | Sell premium — credit spreads only |
| IVR < 45 after Credit entry | Exit Credit | No new entries until re-triggered  |
| IVR < 30                    | Debit       | Buy premium — debit spreads only   |
| IVR > 35 after Debit entry  | Exit Debit  | No new entries until re-triggered  |
| IVR 35 – 45                 | Standby     | No new entries under any condition |

**Gate 2 — Directional Bias (`signals/bias_gate.py`)**

Active only after 10:30 AM. Signal valid for 30 minutes.

| PCR Value   | Signal  | Condition                                        |
| ----------- | ------- | ------------------------------------------------ |
| > 1.35      | Bullish | Proceed to Gate 3                                |
| < 0.75      | Bearish | Proceed to Gate 3 (unless macro override active) |
| 0.75 – 1.35 | Neutral | Evaluation terminated — no entry                 |

Macro override active when `Spot > 20-day SMA of Futures close`. All Bearish signals discarded.

**Gate 3 — Execution Trigger (`signals/trigger_gate.py`)**

| Direction | Trigger Condition                                                 |
| --------- | ----------------------------------------------------------------- |
| Bullish   | One 1-min close confirming Spot is within VWAP Pullback Tolerance |
| Bearish   | One 1-min close confirming Spot is within VWAP Pullback Tolerance |
| Timeout   | Immediate state transition if conditions lost prior to executing  |

### 13.2 Spread Construction

**Iron Condor (Credit Regime):**
Rather than building single-sided calls/puts, the standard strategy payload is a unified Iron Condor to capture structural Theta decay from both directions while defining risk.

**Structure Rules:**

- **CREDIT_PUT Leg:**
  - Short: `ATM - 1×interval`
  - Long: `ATM - 1×interval - (SPREAD_WIDTH × interval)`
- **CREDIT_CALL Leg:**
  - Short: `ATM + 1×interval`
  - Long: `ATM + 1×interval + (SPREAD_WIDTH × interval)`

_Example using `SPREAD_WIDTH=6` and `interval=50`: The wings are 300 points out from the short strikes._

Both legs always submitted as a single atomic basket order. Manual leg sequencing is architecturally forbidden.

### 13.3 Position Sizing Formula

```
Trade Capital  = Total Capital * RISK_PER_TRADE_PCT
Margin Reqmts  = Required margin for 4-Leg Iron Condor + 1.1x scaling buffer
Lots           = floor( Trade Capital / Margin Reqmts )
Stop Distance  = 2.0 × ATR-14  (in index points)
```

If the formula yields zero lots (due to failing the 1.1x margin safety check), the trade is skipped entirely to prevent broker basket rejections.

### 13.4 Process & Thread Model

| Layer                            | Runs In                 | Blocking I/O Permitted |
| -------------------------------- | ----------------------- | ---------------------- |
| WebSocket receive, tick dispatch | Main asyncio event loop | Never                  |
| VWAP running-sum update          | Main asyncio event loop | Never                  |
| Greeks engine, BSM, IVR, ATR     | Separate worker process | Yes — fully isolated   |
| PCR, OI wall, signal gates       | Asyncio coroutines      | Never                  |
| Risk engine, OMS                 | Asyncio coroutines      | Never                  |
| Async write buffer drain         | Background asyncio Task | Via asyncpg only       |
| PostgreSQL writes                | Background asyncio Task | Via asyncpg only       |
| Health watchdog                  | Separate subprocess     | Yes — fully isolated   |
| APScheduler                      | Separate thread         | Yes — fully isolated   |
| Telegram alerter                 | Background asyncio Task | Never                  |

---

## 14. Monitoring & Alerts

### 14.1 Immediate Critical Alerts (fire within seconds)

| Event                                                           | Severity |
| --------------------------------------------------------------- | -------- |
| Circuit breaker tripped (−₹30,000)                              | CRITICAL |
| WebSocket dead (2 consecutive missed heartbeats)                | CRITICAL |
| Authentication failed                                           | CRITICAL |
| Scrip Master parse error or fewer than 20 strike pairs resolved | CRITICAL |
| IV bootstrap failed (fewer than 200 historical records)         | CRITICAL |
| Basket order rejected by broker                                 | HIGH     |
| Margin pre-check failed                                         | HIGH     |
| Expiry weekday changed from previous session                    | HIGH     |
| Data gap exceeding 5 minutes detected during live session       | MEDIUM   |

### 14.2 End-of-Day Report (03:10 PM every trading day)

Delivered via Telegram after the hard kill completes.

```
=== EOD REPORT — [DATE] ===
Mode: Ghost / Live
Net P&L (after costs): ₹ X,XXX
Trades: N  |  Wins: N  |  Losses: N  |  Win Rate: XX%
Largest Single Loss: ₹ X,XXX
IVR Regime Today: Credit / Debit / Standby
Circuit Breaker: Not triggered / TRIGGERED at HH:MM
Sessions to date: N  |  Cumulative P&L: ₹ X,XXX
================================
```

### 14.3 Health Watchdog

A subprocess independent of the main process pings the main bot's in-memory heartbeat counter every 30 seconds. If two consecutive pings show no counter increment, the main process is frozen or crashed and a Telegram CRITICAL alert fires immediately. This watchdog survives a main process crash because it runs in a separate OS process.

---

## 15. Troubleshooting

| Symptom                                                | Likely Cause                                       | Fix                                                                                      |
| ------------------------------------------------------ | -------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Scrip Master parser returns fewer than 20 strike pairs | NSE changed CSV schema                             | Check `data/scrip_master.py` column name mappings against today's CSV                    |
| IV bootstrap fails on startup                          | `iv_history` table empty or fewer than 200 records | Run the jugaad-data bhavcopy download and BSM parser manually                            |
| WebSocket connects but no ticks arrive                 | Subscription token list is empty                   | Verify Scrip Master parsed correctly and token map is populated                          |
| Greeks worker result is always stale                   | Worker process crashed silently                    | Check worker process logs — likely a NumPy/SciPy import error                            |
| Circuit breaker trips on first trade                   | Stop distance too small, lots calculated too high  | Verify ATR value is in index points, not percentage. Check lot size is 65                |
| Basket order rejected every time                       | Margin insufficient                                | Check available margin in MO account. Reduce `MAX_CONCURRENT_POSITIONS` to 1 temporarily |
| Telegram alerts not received                           | Bot token or chat ID incorrect                     | Verify both values in `.env`. Send a test message via Telegram Bot API directly          |
| APScheduler jobs not firing at correct time            | Server timezone not set to IST                     | Run `timedatectl set-timezone Asia/Kolkata` on the server                                |
| Ghost Ledger P&L is positive but unrealistically high  | Slippage buffer too low or costs not applied       | Verify `cost_model.py` is called at fill time in `ghost_ledger.py`                       |

---

## Build Sequence Summary

```
Step 1  →  Obtain all credentials (MO API, Neon DB, Telegram)
Step 2  →  Provision cloud VM, set timezone to IST
Step 3  →  Clone repository, install dependencies
Step 4  →  Configure .env and constants.py (lot sizes: Nifty=65)
Step 5  →  Create PostgreSQL schema (4 tables)
Step 6  →  Download TradingTuitions 1-min intraday data → data_store/raw_csv/
Step 7  →  Download NSE bhavcopy via jugaad-data → populate iv_history table
Step 8  →  Run data_pipeline.py → convert CSV to Parquet
Step 9  →  Run data_cleaner.py → gap detection, forward-fill, day exclusion
Step 10 →  Verify data integrity (10:30 AM candle coverage ≥ 95%)
Step 11 →  Run backtest in-sample (2021 – mid 2023), walk-forward optimisation
Step 12 →  Update constants.py with calibrated thresholds
Step 13 →  Run out-of-sample blind validation (mid 2023 – 2024) — once, no changes
Step 14 →  Confirm pass/fail criteria. If fail, revisit signal gate logic
Step 15 →  Set LIVE_TRADING=False, deploy to cloud VM
Step 16 →  Run Ghost Mode minimum 2 weeks, verify all acceptance criteria
Step 17 →  Set LIVE_TRADING=True
Step 18 →  Monitor manually for first 5 live trading days
Step 19 →  Review live Sharpe vs backtest Sharpe after 30 trading days
```

---

_Last updated: V4.1 — Execution Unity, Database Scalability, reporting, and Hot-Boot Resilience._
