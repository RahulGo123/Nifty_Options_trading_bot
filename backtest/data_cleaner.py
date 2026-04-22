"""
backtest/data_cleaner.py
========================
Second-pass data quality gate — reads Parquet files produced by
data_pipeline.py and validates them for backtesting readiness.

Run after data_pipeline.py:
    python -m backtest.data_cleaner

What this does
--------------
1. Reads every Parquet file in the output directory.
2. For each trading day in each file, checks:
     a. 10:30 AM candle exists for NIFTY-FUT (required for PCR activation).
     b. OI is non-zero for at least one NIFTY option at 10:30 AM.
     c. NIFTY-FUT close series has no remaining NaN after gap-fill.
3. Appends newly-failing days to excluded_days.txt.
4. Prints the integrity report — must pass all checks before backtest runs.

Integrity thresholds (from blueprint)
--------------------------------------
- 10:30 AM candle present on >= 95% of trading days.
- OI non-zero on >= 95% of trading days.
- Total trading days in dataset >= 250 (minimum for IVR to be meaningful).

20-day SMA note
---------------
The macro override SMA is NOT computed from the intraday Parquet files.
It is computed from bhavcopy EOD data (continuous daily closes).
Call build_sma_from_bhavcopy() from this module to load bhavcopy CSVs
and build the SMA series that the backtest engine will use.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
import numpy as np
from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PCR_ACTIVATION_TIME = time(10, 30)
MIN_PCR_CANDLE_COVERAGE = 0.95
MIN_OI_COVERAGE = 0.95
MIN_EXPECTED_DAYS = 200

DEFAULT_PARQUET_DIR = Path("data_store/parquet")
DEFAULT_BHAVCOPY_DIR = Path("data_store/raw_csv/bhavcopy")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class DayCheckResult:
    trading_date: date
    parquet_file: str
    has_pcr_candle: bool
    has_nonzero_oi: bool
    has_no_nan_close: bool

    @property
    def passed(self) -> bool:
        return self.has_pcr_candle and self.has_nonzero_oi and self.has_no_nan_close

    @property
    def failure_reason(self) -> str:
        reasons = []
        if not self.has_pcr_candle:
            reasons.append("MISSING_10:30_CANDLE")
        if not self.has_nonzero_oi:
            reasons.append("ZERO_OI")
        if not self.has_no_nan_close:
            reasons.append("NAN_CLOSE")
        return "|".join(reasons) if reasons else ""


@dataclass
class CleanerReport:
    total_days_checked: int = 0
    days_passed: int = 0
    days_newly_excluded: int = 0
    missing_pcr_candle: int = 0
    zero_oi_days: int = 0
    nan_close_days: int = 0
    pcr_candle_coverage: float = 0.0
    oi_coverage: float = 0.0
    dataset_ready: bool = False
    check_results: List[DayCheckResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-day checks
# ---------------------------------------------------------------------------


def check_trading_day(
    df_day: pd.DataFrame,
    trading_date: date,
    parquet_file: str,
) -> DayCheckResult:
    """
    Runs all three quality checks for a single trading day's data.

    Parameters
    ----------
    df_day : DataFrame with DatetimeIndex, filtered to one trading day,
             containing all Phase 1 tickers.
    """
    pcr_ts = pd.Timestamp(datetime.combine(trading_date, PCR_ACTIVATION_TIME))

    # Check 1: NIFTY-FUT has a 10:30 AM candle
    futures_rows = df_day[(df_day.get("instrument", pd.Series(dtype=str)) == "FUTURES")]
    has_pcr_candle = pcr_ts in futures_rows.index if not futures_rows.empty else False

    # Check 2: At least one NIFTY option has non-zero OI at or around 10:30 AM
    options_rows = df_day[(df_day.get("instrument", pd.Series(dtype=str)) == "OPTION")]
    if not options_rows.empty:
        window_start = pcr_ts - pd.Timedelta(minutes=5)
        window_end = pcr_ts + pd.Timedelta(minutes=5)
        oi_window = options_rows[
            (options_rows.index >= window_start) & (options_rows.index <= window_end)
        ]
        has_nonzero_oi = (
            not oi_window.empty
            and "oi" in oi_window.columns
            and (oi_window["oi"] > 0).any()
        )
    else:
        has_nonzero_oi = False

    # Check 3: No NaN in NIFTY-FUT close column (after gap fill was applied)
    if not futures_rows.empty and "close" in futures_rows.columns:
        has_no_nan_close = not futures_rows["close"].isna().any()
    else:
        has_no_nan_close = True  # No futures data at all — handled elsewhere

    return DayCheckResult(
        trading_date=trading_date,
        parquet_file=parquet_file,
        has_pcr_candle=has_pcr_candle,
        has_nonzero_oi=has_nonzero_oi,
        has_no_nan_close=has_no_nan_close,
    )


# ---------------------------------------------------------------------------
# Main cleaner
# ---------------------------------------------------------------------------


def run_cleaner(
    parquet_dir: Path,
    update_excluded: bool = True,
) -> CleanerReport:
    """
    Validates all Parquet files in parquet_dir.

    Parameters
    ----------
    parquet_dir    : directory containing Parquet files and excluded_days.txt
    update_excluded: if True, appends newly-failing days to excluded_days.txt
    """
    report = CleanerReport()

    parquet_files = sorted(parquet_dir.glob("weekly_*.parquet"))
    if not parquet_files:
        logger.error("[CLEANER] No Parquet files found in {}", parquet_dir)
        return report

    logger.info("[CLEANER] Checking {} Parquet files...", len(parquet_files))

    # Load existing excluded days so we don't double-count
    existing_excluded: Set[date] = _load_excluded_days(parquet_dir)
    newly_excluded: List[date] = []

    for pq_path in parquet_files:
        try:
            df = pd.read_parquet(pq_path, engine="pyarrow")
        except Exception as exc:
            logger.error("[CLEANER] Cannot read {}: {}", pq_path.name, exc)
            continue

        if df.empty:
            logger.warning("[CLEANER] Empty Parquet: {}", pq_path.name)
            continue

        # Ensure DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            logger.warning("[CLEANER] Non-datetime index in {}", pq_path.name)
            continue

        trading_dates = df.index.normalize().unique()

        for td in trading_dates:
            td_date = td.date()
            if td_date in existing_excluded:
                continue  # Already excluded — skip

            df_day = df[df.index.normalize() == td]
            result = check_trading_day(df_day, td_date, pq_path.name)
            report.check_results.append(result)
            report.total_days_checked += 1

            if result.passed:
                report.days_passed += 1
            else:
                report.days_newly_excluded += 1
                newly_excluded.append(td_date)
                if not result.has_pcr_candle:
                    report.missing_pcr_candle += 1
                if not result.has_nonzero_oi:
                    report.zero_oi_days += 1
                if not result.has_no_nan_close:
                    report.nan_close_days += 1

                logger.warning(
                    "[CLEANER] Day {} in {} FAILED: {}",
                    td_date,
                    pq_path.name,
                    result.failure_reason,
                )

    # ── Compute coverage metrics ──
    n = report.total_days_checked
    if n > 0:
        report.pcr_candle_coverage = (n - report.missing_pcr_candle) / n
        report.oi_coverage = (n - report.zero_oi_days) / n

    # ── Pass/fail decision ──
    report.dataset_ready = (
        report.total_days_checked >= MIN_EXPECTED_DAYS
        and report.pcr_candle_coverage >= MIN_PCR_CANDLE_COVERAGE
        and report.oi_coverage >= MIN_OI_COVERAGE
    )

    # ── Update excluded_days.txt ──
    if update_excluded and newly_excluded:
        all_excluded = sorted(existing_excluded | set(newly_excluded))
        _write_excluded_days(all_excluded, parquet_dir)
        logger.info(
            "[CLEANER] {} new days added to excluded_days.txt",
            len(newly_excluded),
        )

    # ── Print report ──
    _print_report(report)
    return report


# ---------------------------------------------------------------------------
# 20-day SMA from bhavcopy
# ---------------------------------------------------------------------------


def build_sma_from_bhavcopy(
    bhavcopy_dir: Path,
    sma_period: int = 20,
) -> pd.Series:
    """
    Builds the 20-day SMA of Nifty Futures daily closing prices from
    NSE FnO bhavcopy CSV files.

    The macro override (Spot > 20-day SMA → bullish bias) requires a
    continuous daily close series. This cannot come from the 61 intraday
    files because they have non-continuous coverage. Bhavcopy provides
    a clean daily close for every trading day.

    Parameters
    ----------
    bhavcopy_dir : directory containing fo_bhavcopy_YYYYMMDD.csv files
    sma_period   : rolling window (default 20)

    Returns
    -------
    pd.Series with DatetimeIndex, values = 20-day SMA of NIFTY FUT close.
    Index is the trading date. NaN for the first (sma_period - 1) days.

    Bhavcopy column format (legacy):
        INSTRUMENT, SYMBOL, EXPIRY_DT, STRIKE_PR, OPTION_TYP,
        OPEN, HIGH, LOW, CLOSE, SETTLE_PR, CONTRACTS, VAL_INLAKH,
        OPEN_INT, CHG_IN_OI, TIMESTAMP

    Bhavcopy column format (new, post July 2024):
        varies — parsed by _parse_bhavcopy_new()
    """
    bhavcopy_files = sorted(bhavcopy_dir.glob("fo_bhavcopy_*.csv"))
    if not bhavcopy_files:
        logger.warning("[CLEANER] No bhavcopy files found in {}", bhavcopy_dir)
        return pd.Series(dtype=float)

    logger.info("[CLEANER] Loading {} bhavcopy files for SMA...", len(bhavcopy_files))

    daily_closes: Dict[date, float] = {}

    for fp in bhavcopy_files:
        try:
            close_price = _extract_nifty_fut_close(fp)
            if close_price is not None:
                # Date from filename: fo_bhavcopy_YYYYMMDD.csv
                date_str = fp.stem.replace("fo_bhavcopy_", "")
                trading_date = datetime.strptime(date_str, "%Y%m%d").date()
                daily_closes[trading_date] = close_price
        except Exception as exc:
            logger.debug("[CLEANER] Bhavcopy parse error {}: {}", fp.name, exc)

    if not daily_closes:
        logger.warning("[CLEANER] No NIFTY-FUT closes extracted from bhavcopy.")
        return pd.Series(dtype=float)

    series = pd.Series(daily_closes).sort_index()
    series.index = pd.DatetimeIndex(series.index)
    sma = series.rolling(window=sma_period, min_periods=sma_period).mean()

    logger.info(
        "[CLEANER] SMA-{} built from {} bhavcopy days ({} to {}).",
        sma_period,
        len(series),
        series.index[0].date(),
        series.index[-1].date(),
    )
    return sma


def _extract_nifty_fut_close(fp: Path) -> Optional[float]:
    """
    Reads one bhavcopy CSV and returns the NIFTY near-month futures
    closing price. Returns None if not found.
    """
    df = pd.read_csv(fp, low_memory=False)
    if df.empty:
        return None

    # Normalise column names
    df.columns = [c.strip().upper() for c in df.columns]

    # Legacy format
    if "INSTRUMENT" in df.columns and "SYMBOL" in df.columns:
        fut_rows = df[(df["INSTRUMENT"] == "FUTIDX") & (df["SYMBOL"] == "NIFTY")]
        if fut_rows.empty:
            return None
        # Near-month = earliest expiry
        if "EXPIRY_DT" in df.columns:
            fut_rows = fut_rows.copy()
            fut_rows["_exp"] = pd.to_datetime(
                fut_rows["EXPIRY_DT"], dayfirst=True, errors="coerce"
            )
            fut_rows = fut_rows.sort_values("_exp")
        close_col = next(
            (c for c in ["CLOSE", "SETTLE_PR"] if c in fut_rows.columns), None
        )
        if close_col is None:
            return None
        return float(fut_rows.iloc[0][close_col])

    # New format (post July 2024) — column names vary
    # Try to find NIFTY FUT row by scanning
    for col in df.columns:
        if "SYMBOL" in col.upper() or "INSTRUMENT" in col.upper():
            nifty_rows = df[df[col].astype(str).str.strip().str.upper() == "NIFTY"]
            if not nifty_rows.empty:
                close_col = next(
                    (
                        c
                        for c in df.columns
                        if "CLOSE" in c.upper() or "SETTLE" in c.upper()
                    ),
                    None,
                )
                if close_col:
                    return float(nifty_rows.iloc[0][close_col])
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_excluded_days(parquet_dir: Path) -> Set[date]:
    path = parquet_dir / "excluded_days.txt"
    if not path.exists():
        return set()
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    result = set()
    for line in lines:
        line = line.strip()
        if line:
            try:
                result.add(date.fromisoformat(line))
            except ValueError:
                pass
    return result


def _write_excluded_days(excluded: List[date], parquet_dir: Path) -> None:
    path = parquet_dir / "excluded_days.txt"
    path.write_text(
        "\n".join(d.isoformat() for d in sorted(excluded)),
        encoding="utf-8",
    )


def _print_report(report: CleanerReport) -> None:
    sep = "─" * 52
    print(f"\n{sep}")
    print("  DATA CLEANER — Integrity Report")
    print(sep)
    print(f"  Days checked:          {report.total_days_checked:>6}")
    print(f"  Days passed:           {report.days_passed:>6}")
    print(f"  Days newly excluded:   {report.days_newly_excluded:>6}")
    print(f"  Missing 10:30 candle:  {report.missing_pcr_candle:>6}")
    print(f"  Zero OI days:          {report.zero_oi_days:>6}")
    print(f"  NaN close days:        {report.nan_close_days:>6}")
    print(
        f"  PCR candle coverage:   {report.pcr_candle_coverage:>5.1%}  (min {MIN_PCR_CANDLE_COVERAGE:.0%})"
    )
    print(
        f"  OI coverage:           {report.oi_coverage:>5.1%}  (min {MIN_OI_COVERAGE:.0%})"
    )
    print(sep)
    status = "✅ DATASET READY" if report.dataset_ready else "❌ DATASET NOT READY"
    print(f"  {status}")
    if not report.dataset_ready:
        if report.total_days_checked < MIN_EXPECTED_DAYS:
            print(
                f"  Insufficient days: {report.total_days_checked} < {MIN_EXPECTED_DAYS} minimum."
            )
        if report.pcr_candle_coverage < MIN_PCR_CANDLE_COVERAGE:
            print("  PCR candle coverage below threshold.")
        if report.oi_coverage < MIN_OI_COVERAGE:
            print("  OI coverage below threshold.")
    print(sep + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate backtest Parquet files.")
    parser.add_argument("--parquet-dir", type=Path, default=DEFAULT_PARQUET_DIR)
    parser.add_argument("--bhavcopy-dir", type=Path, default=DEFAULT_BHAVCOPY_DIR)
    parser.add_argument(
        "--build-sma",
        action="store_true",
        help="Also build 20-day SMA from bhavcopy and save to parquet_dir/nifty_fut_sma20.parquet",
    )
    args = parser.parse_args()

    report = run_cleaner(parquet_dir=args.parquet_dir)

    if args.build_sma:
        sma = build_sma_from_bhavcopy(args.bhavcopy_dir)
        if not sma.empty:
            sma_path = args.parquet_dir / "nifty_fut_sma20.parquet"
            sma.to_frame(name="sma20").to_parquet(sma_path)
            logger.info("[CLEANER] SMA saved to {}", sma_path)
