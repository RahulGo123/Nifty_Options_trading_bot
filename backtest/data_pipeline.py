"""
backtest/data_pipeline.py
==========================
Phase 1A — Converts TradingTuitions 1-minute weekly CSV files to Parquet.

Data format (confirmed from file analysis)
------------------------------------------
Columns: Ticker | Date/Time | Open | High | Low | Close | Volume | Open Interest
Date format: DD-MM-YYYY HH:MM:SS  (dayfirst=True)

Ticker formats:
  NIFTYWK{strike}CE   — Nifty weekly call option
  NIFTYWK{strike}PE   — Nifty weekly put option
  NIFTY-FUT           — Nifty continuous futures (VWAP, ATR)
  NIFTY               — Nifty spot index (macro SMA)
  INDIAVIX            — India VIX (IV cross-check)
  BANKNIFTY*          — Phase 2 — discarded
  Everything else     — discarded

File structure:
  Each file = one weekly expiry cycle (~5 trading days, ~364,000 rows)
  Expiry date is embedded in the filename, not the rows
  61 files total ≈ 22 million rows ≈ 14 months of data

Architecture decision: each file is treated as an independent weekly episode.
The 61 files are NOT concatenated into one time series. Gaps between files
(missing weeks) are simply skipped. P&L accumulates across episodes.
This is correct because the live system never carries positions across expiries.

Filename expiry parsing
-----------------------
Pattern examples:
  01_07_JULY_WEEKLY_expiry_data_VEGE_NF_AND_BNF...  → expiry 07-Jul
  16-22-SEP-WEEKLY-expiry_data_VEGE_NF_AND...        → expiry 22-Sep
  28Jan_03FEB_22_WEEKLY_expiry_data_VEGE_NF...        → expiry 03-Feb-2022

Strategy: extract the LAST date mentioned in the filename stem — that is
always the expiry date (the week ends on expiry day).

Run:
    python -m backtest.data_pipeline
    python -m backtest.data_pipeline --raw-dir data_store/raw_csv/tradingtuitions
                                     --out-dir data_store/parquet
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MARKET_OPEN = time(9, 15)
MARKET_CLOSE = time(15, 30)

FILL_THRESHOLD = 5  # gaps < 5 min → forward-fill
EXCLUDE_THRESHOLD = 5  # gaps >= 5 min → flag window as EXCLUDED
DAY_EXCLUDE_MINS = 30  # total excluded min per day → drop entire day
SIGNAL_WINDOW_PAD = 30  # minutes of padding around an excluded gap
STRIKE_INTERVAL = 50  # Nifty strike interval in points

# Only count gaps within this many strikes of ATM when deciding
# whether to exclude an entire trading day. Far-OTM options
# routinely have gaps — they must not contaminate the day check.
NEAR_ATM_EXCLUSION_RANGE = 5  # ATM ± 5 strikes = ± 250 points for Nifty

PRICE_COLS = ["open", "high", "low", "close"]
VOLUME_COLS = ["volume", "oi"]

# Phase 1 instrument filter — keep only these prefixes/exact names
KEEP_TICKERS = {"NIFTY", "NIFTY-FUT", "INDIAVIX"}  # exact
KEEP_PREFIX = "NIFTYWK"  # Nifty weekly options

MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUNE": 6,
    "JUL": 7,
    "JULY": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

DEFAULT_RAW_DIR = Path("data_store/raw_csv/tradingtuitions")
DEFAULT_OUT_DIR = Path("data_store/parquet")


# ---------------------------------------------------------------------------
# Filename → expiry date
# ---------------------------------------------------------------------------


def parse_expiry_from_filename(stem: str) -> Optional[date]:
    """
    Extracts the expiry date from a TradingTuitions filename stem.

    Strategy
    --------
    1. Extract the year first (4-digit 20xx pattern).
    2. Find all (day, month) pairs — day is a 1-2 digit number,
       month is a 3-6 letter abbreviation from MONTH_MAP.
    3. The last (day, month) pair is the expiry date.

    Handles all observed patterns:
      01_07_JULY_WEEKLY_expiry_data_VEGE_NF_AND_BNF  → Jul 07
      16_22_SEP_WEEKLY_expiry_data_VEGE_NF            → Sep 22
      28Jan_03FEB_22_WEEKLY_expiry_data_VEGE_NF       → Feb 03, year=2022
      14_20_OCT_WEEKLY_expiry_data_VEGE_NF            → Oct 20
      30Jul_to_05_AUG_2021_weekly_ExpiryWEEKdata      → Aug 05, year=2021
      11_17_MAR_WEEKLY_expiry_data_VEGE_NF            → Mar 17
    """
    stem_upper = stem.upper()

    # ── Step 1: Extract year from explicit 4-digit pattern ──
    year_match = re.search(r"(?<![0-9])(20[12][0-9])(?![0-9])", stem_upper)
    if year_match:
        year = int(year_match.group(1))
    else:
        # Try 2-digit year in a clear position like _22_ or _21_
        year_match2 = re.search(r"(?<![A-Z])_?(2[012345])_(?![A-Z])", stem_upper)
        year = int("20" + year_match2.group(1)) if year_match2 else None

    # ── Step 2: Find all (day, month) pairs ──
    # We look for a month name and the nearest 1-2 digit number before or after it.
    # We scan by finding month names first, then look at adjacent digits.
    candidates: List[Tuple[int, int, int]] = []  # (position, day, month_num)

    for mon_name, mon_num in MONTH_MAP.items():
        for m in re.finditer(mon_name, stem_upper):
            pos = m.start()
            # Look for digits immediately before (with optional separator)
            before = stem_upper[:pos].rstrip("_- ")
            day_before = re.search(r"(\d{1,2})$", before)
            # Look for digits immediately after (with optional separator)
            after = stem_upper[pos + len(mon_name) :]
            day_after = re.match(r"[_\- ]?(\d{1,2})", after)

            if day_before:
                d = int(day_before.group(1))
                if 1 <= d <= 31:
                    candidates.append((pos, d, mon_num))
            elif day_after:
                d = int(day_after.group(1))
                if 1 <= d <= 31:
                    candidates.append((pos, d, mon_num))

    if not candidates:
        logger.warning("[PIPELINE] Cannot parse expiry from filename: {}", stem)
        return None

    # ── Step 3: Take the last candidate by position ──
    candidates.sort(key=lambda x: x[0])
    _, last_day, last_month = candidates[-1]

    # ── Step 4: Determine year ──
    if year is None:
        # Cannot determine year — default to a known range check later
        logger.warning("[PIPELINE] No year found in '{}', defaulting to 2022.", stem)
        year = 2022

    try:
        return date(year, last_month, last_day)
    except ValueError:
        logger.warning(
            "[PIPELINE] Invalid date {}-{}-{} from: {}",
            year,
            last_month,
            last_day,
            stem,
        )
        return None


# ---------------------------------------------------------------------------
# Ticker parsing
# ---------------------------------------------------------------------------


def parse_ticker(ticker: str) -> Optional[Dict]:
    """
    Parses a TradingTuitions ticker string into its components.

    Returns dict with keys: symbol, instrument, strike, option_type
    Returns None if ticker should be discarded (Phase 2 / not needed).
    """
    t = ticker.strip().upper()

    # Nifty weekly option: NIFTYWK{strike}CE or NIFTYWK{strike}PE
    if t.startswith("NIFTYWK"):
        m = re.match(r"NIFTYWK(\d+)(CE|PE)$", t)
        if m:
            return {
                "symbol": "NIFTY",
                "instrument": "OPTION",
                "strike": int(m.group(1)),
                "option_type": m.group(2),
            }
        return None  # malformed

    if t == "NIFTY-FUT":
        return {
            "symbol": "NIFTY",
            "instrument": "FUTURES",
            "strike": None,
            "option_type": None,
        }

    if t == "NIFTY":
        return {
            "symbol": "NIFTY",
            "instrument": "SPOT",
            "strike": None,
            "option_type": None,
        }

    if t == "INDIAVIX":
        return {
            "symbol": "INDIAVIX",
            "instrument": "VIX",
            "strike": None,
            "option_type": None,
        }

    # Everything else (BANKNIFTY*, FINNIFTY, USDINR, sector indices) → discard
    return None


# ---------------------------------------------------------------------------
# Gap detection and filling
# ---------------------------------------------------------------------------


def build_minute_grid(trading_date: date) -> pd.DatetimeIndex:
    """Complete 1-minute grid for one trading day: 09:15 to 15:30 (376 bars)."""
    start = pd.Timestamp(datetime.combine(trading_date, MARKET_OPEN))
    end = pd.Timestamp(datetime.combine(trading_date, MARKET_CLOSE))
    return pd.date_range(start=start, end=end, freq="1min")


@dataclass
class GapRecord:
    trading_date: date
    ticker: str
    gap_start: datetime
    gap_end: datetime
    gap_minutes: int
    action: str  # "FORWARD_FILL" | "EXCLUDED"
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None


def detect_and_fill_gaps(
    df: pd.DataFrame,
    trading_date: date,
    ticker: str,
) -> Tuple[pd.DataFrame, List[GapRecord]]:
    """
    Reindexes a single-ticker, single-day DataFrame to the complete 1-minute
    grid and applies gap rules.
    """
    grid = build_minute_grid(trading_date)
    df = df.reindex(grid)

    records: List[GapRecord] = []
    null_mask = df["close"].isna() if "close" in df.columns else df.iloc[:, 0].isna()

    # Find contiguous NaN runs
    gaps: List[Tuple[int, int]] = []
    in_gap = False
    gap_start_idx = 0
    for i, is_null in enumerate(null_mask):
        if is_null and not in_gap:
            gap_start_idx = i
            in_gap = True
        elif not is_null and in_gap:
            gaps.append((gap_start_idx, i - 1))
            in_gap = False
    if in_gap:
        gaps.append((gap_start_idx, len(null_mask) - 1))

    day_open = pd.Timestamp(datetime.combine(trading_date, MARKET_OPEN))
    day_close = pd.Timestamp(datetime.combine(trading_date, MARKET_CLOSE))

    for s_idx, e_idx in gaps:
        gap_len = e_idx - s_idx + 1
        gs = grid[s_idx].to_pydatetime()
        ge = grid[e_idx].to_pydatetime()

        if gap_len < FILL_THRESHOLD:
            for col in PRICE_COLS:
                if col in df.columns:
                    df[col] = df[col].ffill()
            for col in VOLUME_COLS:
                if col in df.columns:
                    df[col] = df[col].fillna(0)
            records.append(
                GapRecord(trading_date, ticker, gs, ge, gap_len, "FORWARD_FILL")
            )
        else:
            # Large gap — flag surrounding window as EXCLUDED.
            # Exception: gaps that END before 09:30 AM do NOT generate an
            # exclusion window. The execution window opens at 09:30, so data
            # quality before that time does not affect whether signals can fire.
            # Options market makers take several minutes to post quotes after
            # 09:15 open — a 5-10 bar gap in the first 15 minutes is normal.
            execution_open = datetime.combine(trading_date, time(9, 30))
            if ge < execution_open:
                # Gap is entirely in pre-execution period — forward-fill and move on
                for col in PRICE_COLS:
                    if col in df.columns:
                        df[col] = df[col].ffill()
                for col in VOLUME_COLS:
                    if col in df.columns:
                        df[col] = df[col].fillna(0)
                records.append(
                    GapRecord(trading_date, ticker, gs, ge, gap_len, "FORWARD_FILL")
                )
            else:
                ws = max(
                    pd.Timestamp(gs - timedelta(minutes=SIGNAL_WINDOW_PAD)), day_open
                )
                we = min(
                    pd.Timestamp(ge + timedelta(minutes=SIGNAL_WINDOW_PAD)), day_close
                )
                records.append(
                    GapRecord(
                        trading_date,
                        ticker,
                        gs,
                        ge,
                        gap_len,
                        "EXCLUDED",
                        ws.to_pydatetime(),
                        we.to_pydatetime(),
                    )
                )

    return df, records


def total_excluded_minutes(records: List[GapRecord]) -> int:
    """Minutes inside EXCLUDED windows for one day, after merging overlaps."""
    windows = [
        (r.window_start, r.window_end)
        for r in records
        if r.action == "EXCLUDED" and r.window_start and r.window_end
    ]
    if not windows:
        return 0
    windows.sort(key=lambda w: w[0])
    merged = [list(windows[0])]
    for s, e in windows[1:]:
        if s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    return sum(int((e - s).total_seconds() / 60) + 1 for s, e in merged)


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    files_processed: int = 0
    files_failed: int = 0
    days_excluded: int = 0
    total_gaps: int = 0
    filled_gaps: int = 0
    excluded_gaps: int = 0
    gap_records: List[GapRecord] = field(default_factory=list)
    excluded_days: List[date] = field(default_factory=list)
    parquet_files: List[Path] = field(default_factory=list)


def process_file(
    csv_path: Path,
    out_dir: Path,
) -> Tuple[bool, List[GapRecord], List[date]]:
    """
    Processes one TradingTuitions weekly CSV file.

    Steps
    -----
    1. Parse expiry date from filename.
    2. Read CSV — columns: Ticker, Date/Time, Open, High, Low, Close,
       Volume, Open Interest.
    3. Parse date column with dayfirst=True.
    4. Parse ticker → symbol/instrument/strike/option_type.
    5. Filter: keep Phase 1 instruments only.
    6. For each (ticker, trading_date) group: reindex to 1-min grid,
       detect/fill gaps.
    7. Write one Parquet file per weekly episode.

    Returns (success, gap_records, excluded_days).
    """
    expiry = parse_expiry_from_filename(csv_path.stem)
    if expiry is None:
        logger.error("[PIPELINE] Cannot determine expiry for: {}", csv_path.name)
        return False, [], []

    logger.info("[PIPELINE] Processing {} (expiry: {})", csv_path.name, expiry)

    # ── Read CSV ──
    try:
        df_raw = pd.read_csv(csv_path, low_memory=False)
    except Exception as exc:
        logger.error("[PIPELINE] Read failed {}: {}", csv_path.name, exc)
        return False, [], []

    # ── Normalise column names ──
    col_map = {}
    for col in df_raw.columns:
        cl = col.strip().lower().replace(" ", "_").replace("/", "_")
        if cl in ("ticker", "symbol"):
            col_map[col] = "ticker"
        elif cl in ("date_time", "datetime", "date"):
            col_map[col] = "datetime"
        elif cl == "open":
            col_map[col] = "open"
        elif cl == "high":
            col_map[col] = "high"
        elif cl == "low":
            col_map[col] = "low"
        elif cl in ("close", "ltp"):
            col_map[col] = "close"
        elif cl in ("volume", "vol"):
            col_map[col] = "volume"
        elif cl in ("open_interest", "openinterest", "oi"):
            col_map[col] = "oi"
    df_raw = df_raw.rename(columns=col_map)

    required = {"ticker", "datetime", "open", "high", "low", "close"}
    missing = required - set(df_raw.columns)
    if missing:
        logger.error("[PIPELINE] Missing columns {} in {}", missing, csv_path.name)
        return False, [], []

    if "volume" not in df_raw.columns:
        df_raw["volume"] = 0
    if "oi" not in df_raw.columns:
        df_raw["oi"] = 0

    # ── Parse datetime ──
    df_raw["datetime"] = pd.to_datetime(
        df_raw["datetime"], dayfirst=True, errors="coerce"
    )
    df_raw = df_raw.dropna(subset=["datetime"])

    # ── Parse tickers and filter Phase 1 only (vectorised — no iterrows) ──
    df_raw["ticker"] = df_raw["ticker"].astype(str).str.strip().str.upper()

    # Extract instrument type, strike, option_type from ticker using regex
    # NIFTYWK{strike}CE/PE
    opt_mask = df_raw["ticker"].str.match(r"^NIFTYWK\d+(CE|PE)$")
    fut_mask = df_raw["ticker"] == "NIFTY-FUT"
    spot_mask = df_raw["ticker"] == "NIFTY"
    vix_mask = df_raw["ticker"] == "INDIAVIX"
    keep_mask = opt_mask | fut_mask | spot_mask | vix_mask

    df_raw = df_raw[keep_mask].copy()
    if df_raw.empty:
        logger.warning("[PIPELINE] No Phase 1 rows in {}", csv_path.name)
        return False, [], []

    # Assign instrument type
    df_raw["instrument"] = "OPTION"
    df_raw.loc[fut_mask[keep_mask].values, "instrument"] = "FUTURES"
    df_raw.loc[spot_mask[keep_mask].values, "instrument"] = "SPOT"
    df_raw.loc[vix_mask[keep_mask].values, "instrument"] = "VIX"

    # Parse strike and option_type from option tickers
    df_raw["strike"] = None
    df_raw["option_type"] = None
    df_raw["symbol"] = "NIFTY"
    df_raw.loc[vix_mask[keep_mask].values, "symbol"] = "INDIAVIX"

    opt_tickers = df_raw[df_raw["instrument"] == "OPTION"]["ticker"]
    if not opt_tickers.empty:
        extracted = opt_tickers.str.extract(r"^NIFTYWK(\d+)(CE|PE)$")
        df_raw.loc[df_raw["instrument"] == "OPTION", "strike"] = (
            extracted[0].astype(float).astype("Int64")
        )
        df_raw.loc[df_raw["instrument"] == "OPTION", "option_type"] = extracted[1]

    df_raw["expiry"] = expiry

    # Set datetime index
    df_raw = df_raw.set_index("datetime").sort_index()

    # Rename for consistency
    df = df_raw

    # ── Process each ticker × trading_date group ──
    all_gap_records: List[GapRecord] = []
    all_excluded_days: List[date] = []
    processed_groups: List[pd.DataFrame] = []

    trading_dates = df.index.normalize().unique()

    for td in trading_dates:
        td_date = td.date()
        day_start = pd.Timestamp(datetime.combine(td_date, MARKET_OPEN))
        day_end = pd.Timestamp(datetime.combine(td_date, MARKET_CLOSE))
        df_day = df[(df.index >= day_start) & (df.index <= day_end)]

        if df_day.empty:
            continue

        day_gap_records: List[GapRecord] = []

        for ticker_val in df_day["ticker"].unique():
            df_ticker = df_day[df_day["ticker"] == ticker_val][
                ["open", "high", "low", "close", "volume", "oi"]
            ].copy()

            df_ticker_filled, gap_recs = detect_and_fill_gaps(
                df_ticker, td_date, ticker_val
            )
            day_gap_records.extend(gap_recs)

            # Re-attach ticker metadata after reindexing
            # Metadata already parsed — read from original df_day row
            sample = df_day[df_day["ticker"] == ticker_val].iloc[0]
            df_ticker_filled["ticker"] = ticker_val
            df_ticker_filled["expiry"] = expiry
            df_ticker_filled["trading_date"] = td_date
            df_ticker_filled["symbol"] = sample.get("symbol", "NIFTY")
            df_ticker_filled["instrument"] = sample.get("instrument", "OPTION")
            df_ticker_filled["strike"] = sample.get("strike", None)
            df_ticker_filled["option_type"] = sample.get("option_type", None)

            processed_groups.append(df_ticker_filled)

        all_gap_records.extend(day_gap_records)

        # ── Day exclusion: only count gaps in critical instruments ──
        #
        # Critical instruments for day exclusion:
        #   1. NIFTY-FUT  — gaps break VWAP and ATR
        #   2. NIFTY spot — gaps break ATM calculation
        #   3. NIFTY options within ATM ± NEAR_ATM_EXCLUSION_RANGE strikes
        #
        # Far-OTM options routinely have gaps all day. This is normal
        # illiquidity, not a data quality problem. They must not trigger
        # day exclusion.
        #
        # Implementation: build the set of CRITICAL TICKERS for this day
        # from the DataFrame directly, then filter gap records by that set.

        # Step 1: find critical tickers for this day using the parsed DataFrame
        critical_tickers: set = set()

        # Always critical: NIFTY-FUT and NIFTY spot
        for instr_type in ("FUTURES", "SPOT", "VIX"):
            mask = df_day["instrument"] == instr_type
            critical_tickers.update(df_day.loc[mask, "ticker"].unique())

        # Near-ATM options: estimate ATM from NIFTY spot median close
        spot_mask = df_day["instrument"] == "SPOT"
        if spot_mask.any():
            approx_spot = float(df_day.loc[spot_mask, "close"].median())
        else:
            # Fall back to NIFTY-FUT if spot not present
            fut_mask = df_day["instrument"] == "FUTURES"
            approx_spot = (
                float(df_day.loc[fut_mask, "close"].median()) if fut_mask.any() else 0.0
            )

        if approx_spot > 0:
            approx_atm = round(approx_spot / STRIKE_INTERVAL) * STRIKE_INTERVAL

            # Build near-ATM ticker set using a ticker-level lookup.
            # Working row-by-row on the full minute DataFrame causes false
            # positives because timestamps are shared across all tickers.
            # Instead: deduplicate to one row per ticker, filter by strike
            # distance, then extract the ticker strings.
            opt_rows = df_day[df_day["instrument"] == "OPTION"]
            if not opt_rows.empty:
                # One row per ticker (first occurrence) — preserves ticker/strike mapping
                ticker_strike = (
                    opt_rows[["ticker", "strike"]]
                    .drop_duplicates(subset="ticker")
                    .copy()
                )
                ticker_strike["strike_num"] = pd.to_numeric(
                    ticker_strike["strike"], errors="coerce"
                )
                near_mask = (
                    ticker_strike["strike_num"] - approx_atm
                ).abs() <= NEAR_ATM_EXCLUSION_RANGE * STRIKE_INTERVAL
                critical_tickers.update(ticker_strike.loc[near_mask, "ticker"].tolist())

        # Step 2: filter gap records to critical tickers only
        critical_gap_records = [
            r
            for r in day_gap_records
            if r.trading_date == td_date and r.ticker in critical_tickers
        ]

        excl_mins = total_excluded_minutes(critical_gap_records)
        if excl_mins > DAY_EXCLUDE_MINS:
            logger.warning(
                "[PIPELINE] Day {} excluded: {} critical excluded minutes > {} threshold "
                "(ATM≈{}, critical tickers: {}, excl gap tickers: {}).",
                td_date,
                excl_mins,
                DAY_EXCLUDE_MINS,
                approx_atm if approx_spot > 0 else "?",
                len(critical_tickers),
                len(set(r.ticker for r in critical_gap_records)),
            )
            all_excluded_days.append(td_date)

    if not processed_groups:
        return False, all_gap_records, all_excluded_days

    # ── Combine and write Parquet ──
    df_out = pd.concat(processed_groups)
    df_out = df_out.sort_index()

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"weekly_{expiry.isoformat()}_{csv_path.stem[:20]}.parquet"

    df_out.to_parquet(out_path, engine="pyarrow", compression="snappy", index=True)
    logger.info("[PIPELINE] Written: {} ({:,} rows)", out_path.name, len(df_out))

    return True, all_gap_records, all_excluded_days


def run_pipeline(raw_dir: Path, out_dir: Path) -> PipelineResult:
    """Master function — processes all 61 CSV files."""
    result = PipelineResult()

    csv_files = sorted(raw_dir.rglob("*.csv"))
    if not csv_files:
        logger.error("[PIPELINE] No CSV files found in {}", raw_dir)
        return result

    logger.info("[PIPELINE] Found {} CSV files.", len(csv_files))

    for csv_path in csv_files:
        ok, gap_recs, excl_days = process_file(csv_path, out_dir)

        if ok:
            result.files_processed += 1
            result.parquet_files.append(
                out_dir / f"weekly_{csv_path.stem[:20]}.parquet"
            )
        else:
            result.files_failed += 1

        result.gap_records.extend(gap_recs)
        result.total_gaps += len(gap_recs)
        result.filled_gaps += sum(1 for g in gap_recs if g.action == "FORWARD_FILL")
        result.excluded_gaps += sum(1 for g in gap_recs if g.action == "EXCLUDED")
        result.excluded_days.extend(excl_days)
        result.days_excluded += len(excl_days)

    # ── Write reports ──
    _write_quality_report(result.gap_records, out_dir)
    _write_excluded_days(result.excluded_days, out_dir)

    logger.info(
        "[PIPELINE] Complete. "
        "Processed: {} | Failed: {} | "
        "Gaps: {} (filled: {} excl: {}) | "
        "Days excluded: {}",
        result.files_processed,
        result.files_failed,
        result.total_gaps,
        result.filled_gaps,
        result.excluded_gaps,
        result.days_excluded,
    )
    return result


def _write_quality_report(gap_records: List[GapRecord], out_dir: Path) -> None:
    if not gap_records:
        return
    rows = [
        {
            "trading_date": r.trading_date.isoformat(),
            "ticker": r.ticker,
            "gap_start": r.gap_start.strftime("%Y-%m-%d %H:%M"),
            "gap_end": r.gap_end.strftime("%Y-%m-%d %H:%M"),
            "gap_minutes": r.gap_minutes,
            "action": r.action,
            "window_start": (
                r.window_start.strftime("%Y-%m-%d %H:%M") if r.window_start else ""
            ),
            "window_end": (
                r.window_end.strftime("%Y-%m-%d %H:%M") if r.window_end else ""
            ),
        }
        for r in gap_records
    ]
    path = out_dir / "data_quality_report.csv"
    pd.DataFrame(rows).to_csv(path, index=False)
    logger.info("[PIPELINE] Quality report: {}", path)


def _write_excluded_days(excluded: List[date], out_dir: Path) -> None:
    path = out_dir / "excluded_days.txt"
    path.write_text(
        "\n".join(d.isoformat() for d in sorted(set(excluded))),
        encoding="utf-8",
    )
    logger.info("[PIPELINE] Excluded days: {} ({} days)", path, len(set(excluded)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TradingTuitions CSV → Parquet.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    r = run_pipeline(raw_dir=args.raw_dir, out_dir=args.out_dir)
    if r.files_processed == 0:
        sys.exit(1)
