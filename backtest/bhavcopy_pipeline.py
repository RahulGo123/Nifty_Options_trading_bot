"""
backtest/bhavcopy_pipeline.py
==============================
Converts NSE F&O Bhavcopy CSV files into weekly Parquet files.

Fixes in this version
---------------------
FIX 1 — Episode window widened from 7 to 10 calendar days.
    The 7-day window was too narrow — holidays caused episodes to have
    only 2-3 trading days, blocking entries via min_days_to_expiry.
    10 days reliably captures Mon-Thu of expiry week even with holidays.

FIX 2 — Total OI used for PCR instead of CHG_IN_OI.
    CHG_IN_OI is frequently negative or zero on unwind days (especially
    near expiry), making PCR = 0/0 = 1.0 (neutral) → Gate 2 never fires.
    Total OPEN_INT is always positive and gives a stable, meaningful PCR.
    PCR = Σ(Put OPEN_INT ATM±5) / Σ(Call OPEN_INT ATM±5).
    This is what the majority of traders use for daily PCR.

FIX 3 — IV estimation improved.
    BSM ATM approximation breaks down near expiry (1-2 DTE) and produces
    wildly inflated IV. Now uses a 5-strike average around ATM rather
    than only the exact ATM strike, and clamps to realistic range (5%-80%).
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STRIKE_INTERVAL = 50
LOT_SIZE = 65
BAR_TIME = time(10, 30)  # synthetic bar timestamp
ATM_RANGE_STRIKES = 15  # ATM ± 15 strikes written to Parquet
MIN_VALID_STRIKES = 8  # minimum strikes with valid close
EPISODE_WINDOW_DAYS = 10  # FIX 1: calendar days before expiry

DEFAULT_BHAVCOPY_DIR = Path("data_store/raw_csv/bhavcopy")
DEFAULT_OUT_DIR = Path("data_store/parquet")


# ---------------------------------------------------------------------------
# Bhavcopy reader
# ---------------------------------------------------------------------------


def read_bhavcopy(path: Path) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception as exc:
        logger.warning("[BHAVCOPY] Cannot read {}: {}", path.name, exc)
        return None

    df.columns = (
        df.columns.str.strip().str.upper().str.replace(" ", "_").str.replace("-", "_")
    )

    rename_map = {
        "INSTRUMENT": "INSTRUMENT_TYPE",
        "EXPIRY_DT": "EXPIRY_DT",
        "STRIKE_PR": "STRIKE_PR",
        "OPTION_TYP": "OPTION_TYPE",
        "OPEN_INT": "OPEN_INT",
        "CHG_IN_OI": "CHG_IN_OI",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    required = {
        "INSTRUMENT_TYPE",
        "SYMBOL",
        "EXPIRY_DT",
        "STRIKE_PR",
        "OPTION_TYPE",
        "CLOSE",
        "OPEN_INT",
    }
    if required - set(df.columns):
        logger.warning(
            "[BHAVCOPY] Missing columns in {}: {}",
            path.name,
            required - set(df.columns),
        )
        return None

    df["EXPIRY_DT"] = pd.to_datetime(df["EXPIRY_DT"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["EXPIRY_DT"])
    df["EXPIRY_DT"] = df["EXPIRY_DT"].dt.date

    # Parse trading date from filename
    fname = path.stem
    m = re.search(r"(\d{8})", fname)
    if not m:
        logger.warning("[BHAVCOPY] No date in filename: {}", path.name)
        return None
    try:
        trading_date = datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None

    df["trading_date"] = trading_date

    for col in [
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "SETTLE_PR",
        "OPEN_INT",
        "CHG_IN_OI",
        "STRIKE_PR",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()
    df["INSTRUMENT_TYPE"] = df["INSTRUMENT_TYPE"].astype(str).str.strip().str.upper()
    df["OPTION_TYPE"] = df["OPTION_TYPE"].astype(str).str.strip().str.upper()

    return df


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------


def extract_nifty_weekly_options(df: pd.DataFrame, weekly_expiry: date) -> pd.DataFrame:
    mask = (
        (df["INSTRUMENT_TYPE"] == "OPTIDX")
        & (df["SYMBOL"] == "NIFTY")
        & (df["EXPIRY_DT"] == weekly_expiry)
        & (df["OPTION_TYPE"].isin(["CE", "PE"]))
        & (df["STRIKE_PR"] > 0)
    )
    rows = df[mask][
        ["STRIKE_PR", "OPTION_TYPE", "CLOSE", "SETTLE_PR", "OPEN_INT", "CHG_IN_OI"]
    ].copy()

    # Use SETTLE_PR when CLOSE is zero (expiry day)
    rows["CLOSE"] = np.where(rows["CLOSE"] > 0, rows["CLOSE"], rows["SETTLE_PR"])

    rows = rows.rename(
        columns={
            "STRIKE_PR": "strike",
            "OPTION_TYPE": "option_type",
            "CLOSE": "close",
            "OPEN_INT": "open_int",
            "CHG_IN_OI": "chg_in_oi",
        }
    )
    rows["strike"] = rows["strike"].astype(int)
    return rows.reset_index(drop=True)


def extract_nifty_futures(df: pd.DataFrame) -> Optional[float]:
    mask = (df["INSTRUMENT_TYPE"] == "FUTIDX") & (df["SYMBOL"] == "NIFTY")
    fut = df[mask].sort_values("EXPIRY_DT")
    if fut.empty:
        return None
    close = float(fut["CLOSE"].iloc[0])
    if close <= 0:
        close = float(fut["SETTLE_PR"].iloc[0]) if "SETTLE_PR" in fut.columns else 0.0
    return close if close > 0 else None


def estimate_iv(
    options: pd.DataFrame,
    spot: float,
    expiry: date,
    trading_date: date,
) -> float:
    """
    FIX 3: IV estimation using average of 3 near-ATM strikes.
    BSM ATM approximation: C ≈ S × σ × √T × 0.4  →  σ ≈ C / (S × 0.4 × √T)

    Uses 3 strikes centred on ATM to reduce noise from any single strike.
    Clamps output to 5%-80% (realistic INDIAVIX range for Nifty).
    Returns 0.0 if cannot estimate (engine will use previous bar's IV).
    """
    tte_days = max((expiry - trading_date).days, 1)
    tte_years = tte_days / 365.0
    if tte_years <= 0 or spot <= 0:
        return 0.0

    atm = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL

    # Use ATM and one strike either side for robustness
    strikes_to_try = [atm - STRIKE_INTERVAL, atm, atm + STRIKE_INTERVAL]
    ivs = []
    for s in strikes_to_try:
        ce = options[(options["strike"] == s) & (options["option_type"] == "CE")]
        if ce.empty:
            continue
        prem = float(ce["close"].iloc[0])
        if prem <= 0:
            continue
        iv = (prem / spot) / (0.4 * (tte_years**0.5))
        if 0.05 <= iv <= 0.80:  # clamp to realistic range
            ivs.append(iv)

    return round(float(np.median(ivs)), 4) if ivs else 0.0


# ---------------------------------------------------------------------------
# Episode bar builder
# ---------------------------------------------------------------------------


def build_episode_bars(
    trading_days: List[Tuple[date, pd.DataFrame]],
    weekly_expiry: date,
    excluded_days: Set[date],
) -> pd.DataFrame:
    """
    One bar per trading day at 10:30 AM.

    PCR uses OPEN_INT (total OI) — FIX 2.
    Total OI is always positive and gives a stable ratio.
    PCR = Σ Put OPEN_INT(ATM±5) / Σ Call OPEN_INT(ATM±5).

    The engine's compute_pcr subtracts oi_baseline.
    Since bhavcopy has no 09:15 bar, oi_baseline = {} → delta = OPEN_INT.
    This is the correct behaviour for total-OI PCR.
    """
    all_rows = []

    for trading_date, bhav_df in trading_days:
        if trading_date in excluded_days or trading_date > weekly_expiry:
            continue

        options = extract_nifty_weekly_options(bhav_df, weekly_expiry)
        fut_close = extract_nifty_futures(bhav_df)
        spot = fut_close if (fut_close and fut_close > 0) else None

        if spot is None:
            # Fallback: median of high-OI strikes
            if not options.empty:
                top = options[options["option_type"] == "PE"].nlargest(3, "open_int")
                if not top.empty:
                    spot = float(top["strike"].median())
            if spot is None:
                continue

        if options.empty:
            continue

        # Filter to ATM ± ATM_RANGE_STRIKES
        atm = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL
        min_strike = atm - ATM_RANGE_STRIKES * STRIKE_INTERVAL
        max_strike = atm + ATM_RANGE_STRIKES * STRIKE_INTERVAL
        options = options[
            (options["strike"] >= min_strike) & (options["strike"] <= max_strike)
        ].copy()

        if len(options[options["close"] > 0]) < MIN_VALID_STRIKES:
            continue

        iv_approx = estimate_iv(options, spot, weekly_expiry, trading_date)
        bar_ts = pd.Timestamp(datetime.combine(trading_date, BAR_TIME))

        # FUTURES bar
        if fut_close and fut_close > 0:
            all_rows.append(
                {
                    "datetime": bar_ts,
                    "open": fut_close,
                    "high": fut_close,
                    "low": fut_close,
                    "close": fut_close,
                    "volume": 10_000,
                    "oi": 0,
                    "ticker": "NIFTY-FUT",
                    "expiry": weekly_expiry,
                    "trading_date": trading_date,
                    "symbol": "NIFTY",
                    "instrument": "FUTURES",
                    "strike": None,
                    "option_type": None,
                }
            )

        # SPOT bar
        all_rows.append(
            {
                "datetime": bar_ts,
                "open": spot,
                "high": spot,
                "low": spot,
                "close": spot,
                "volume": 0,
                "oi": 0,
                "ticker": "NIFTY",
                "expiry": weekly_expiry,
                "trading_date": trading_date,
                "symbol": "NIFTY",
                "instrument": "SPOT",
                "strike": None,
                "option_type": None,
            }
        )

        # VIX bar (iv_approx × 100 matches TradingTuitions format)
        if iv_approx > 0:
            all_rows.append(
                {
                    "datetime": bar_ts,
                    "open": iv_approx * 100,
                    "high": iv_approx * 100,
                    "low": iv_approx * 100,
                    "close": iv_approx * 100,
                    "volume": 0,
                    "oi": 0,
                    "ticker": "INDIAVIX",
                    "expiry": weekly_expiry,
                    "trading_date": trading_date,
                    "symbol": "INDIAVIX",
                    "instrument": "VIX",
                    "strike": None,
                    "option_type": None,
                }
            )

        # OPTION bars — FIX 2: write OPEN_INT into oi field for PCR
        for _, opt in options.iterrows():
            if float(opt["close"]) <= 0:
                continue
            all_rows.append(
                {
                    "datetime": bar_ts,
                    "open": float(opt["close"]),
                    "high": float(opt["close"]),
                    "low": float(opt["close"]),
                    "close": float(opt["close"]),
                    "volume": 0,
                    "oi": float(opt["open_int"]),  # TOTAL OI — FIX 2
                    "ticker": f"NIFTYWK{int(opt['strike'])}{opt['option_type']}",
                    "expiry": weekly_expiry,
                    "trading_date": trading_date,
                    "symbol": "NIFTY",
                    "instrument": "OPTION",
                    "strike": int(opt["strike"]),
                    "option_type": str(opt["option_type"]),
                }
            )

    if not all_rows:
        return pd.DataFrame()

    df_out = pd.DataFrame(all_rows).set_index("datetime").sort_index()
    return df_out


# ---------------------------------------------------------------------------
# Expiry discovery
# ---------------------------------------------------------------------------


def find_weekly_expiries(
    bhav_dfs: Dict[date, pd.DataFrame],
    start_date: date,
    end_date: date,
) -> List[date]:
    expiry_dates: Set[date] = set()
    for td, df in bhav_dfs.items():
        if not (start_date <= td <= end_date):
            continue
        mask = (
            (df["INSTRUMENT_TYPE"] == "OPTIDX")
            & (df["SYMBOL"] == "NIFTY")
            & (df["OPTION_TYPE"].isin(["CE", "PE"]))
        )
        for exp in df.loc[mask, "EXPIRY_DT"].unique():
            if isinstance(exp, date) and start_date <= exp <= end_date:
                expiry_dates.add(exp)

    # Thursday = 3, Friday = 4 (post-Nov 2024 NSE moved to Thursday)
    weekly = [e for e in expiry_dates if e.weekday() in (3, 4)]
    if len(weekly) < 10:
        logger.warning(
            "[BHAVCOPY] Only {} weekly expiries by weekday. Using all.", len(weekly)
        )
        weekly = sorted(expiry_dates)

    return sorted(set(weekly))


# ---------------------------------------------------------------------------
# SMA builder
# ---------------------------------------------------------------------------


def build_sma_from_bhavcopy(
    bhav_dfs: Dict[date, pd.DataFrame], out_dir: Path, window: int = 20
) -> None:
    records = []
    for td in sorted(bhav_dfs.keys()):
        fc = extract_nifty_futures(bhav_dfs[td])
        if fc and fc > 0:
            records.append({"date": pd.Timestamp(td), "close": fc})

    if not records:
        logger.warning("[BHAVCOPY] No futures closes — SMA not built.")
        return

    sma_df = pd.DataFrame(records).set_index("date").sort_index()
    sma_df["sma20"] = sma_df["close"].rolling(window=window, min_periods=1).mean()
    out_path = out_dir / "nifty_fut_sma20.parquet"
    sma_df[["sma20"]].to_parquet(out_path, engine="pyarrow", compression="snappy")
    logger.info(
        "[BHAVCOPY] SMA20: {} days ({} to {})",
        len(sma_df),
        sma_df.index[0].date(),
        sma_df.index[-1].date(),
    )


# ---------------------------------------------------------------------------
# Master pipeline
# ---------------------------------------------------------------------------


def run_bhavcopy_pipeline(
    bhavcopy_dir: Path,
    out_dir: Path,
    start_date: date,
    end_date: date,
    excluded_days: Set[date],
    overwrite: bool = False,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(bhavcopy_dir.glob("*.csv"))
    if not csv_files:
        logger.error("[BHAVCOPY] No CSV files in {}", bhavcopy_dir)
        return

    logger.info("[BHAVCOPY] Reading {} bhavcopy files...", len(csv_files))

    bhav_dfs: Dict[date, pd.DataFrame] = {}
    for fp in csv_files:
        df = read_bhavcopy(fp)
        if df is None:
            continue
        td = df["trading_date"].iloc[0]
        if not (start_date <= td <= end_date) or td in excluded_days:
            continue
        bhav_dfs[td] = df

    if not bhav_dfs:
        logger.error("[BHAVCOPY] No valid data for {} to {}", start_date, end_date)
        return

    logger.info(
        "[BHAVCOPY] Loaded {} trading days ({} to {})",
        len(bhav_dfs),
        min(bhav_dfs),
        max(bhav_dfs),
    )

    all_expiries = find_weekly_expiries(bhav_dfs, start_date, end_date)
    logger.info("[BHAVCOPY] {} weekly expiries found.", len(all_expiries))

    written = skipped = 0
    bad_days: Set[date] = set()

    for expiry in all_expiries:
        out_path = out_dir / f"weekly_{expiry.isoformat()}_bhavcopy.parquet"

        if out_path.exists() and not overwrite:
            skipped += 1
            continue

        # FIX 1: 10-day window instead of 7
        window_start = expiry - timedelta(days=EPISODE_WINDOW_DAYS)
        episode_days = [
            (td, df)
            for td, df in sorted(bhav_dfs.items())
            if window_start <= td <= expiry
        ]

        if not episode_days:
            skipped += 1
            continue

        df_ep = build_episode_bars(episode_days, expiry, excluded_days)

        if df_ep.empty:
            for td, _ in episode_days:
                bad_days.add(td)
            skipped += 1
            continue

        if df_ep["trading_date"].nunique() < 2:
            skipped += 1
            continue

        df_ep.to_parquet(out_path, engine="pyarrow", compression="snappy", index=True)
        written += 1
        logger.info(
            "[BHAVCOPY] Written: {} ({} days, {:,} rows)",
            out_path.name,
            df_ep["trading_date"].nunique(),
            len(df_ep),
        )

    logger.info("[BHAVCOPY] Building SMA...")
    build_sma_from_bhavcopy(bhav_dfs, out_dir)

    # Update excluded_days.txt
    excl_path = out_dir / "excluded_days.txt"
    existing: Set[date] = set()
    if excl_path.exists():
        for line in excl_path.read_text(encoding="utf-8").strip().splitlines():
            try:
                existing.add(date.fromisoformat(line.strip()))
            except ValueError:
                pass
    excl_path.write_text(
        "\n".join(d.isoformat() for d in sorted(existing | bad_days)), encoding="utf-8"
    )

    logger.info(
        "[BHAVCOPY] Done. Written: {} | Skipped: {} | Bad days: {}",
        written,
        skipped,
        len(bad_days),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bhavcopy-dir", type=Path, default=DEFAULT_BHAVCOPY_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2024, 12, 31))
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--excluded", type=Path, default=Path("data_store/parquet/excluded_days.txt")
    )
    args = parser.parse_args()

    excluded: Set[date] = set()
    if args.excluded.exists():
        for line in args.excluded.read_text(encoding="utf-8").strip().splitlines():
            try:
                excluded.add(date.fromisoformat(line.strip()))
            except ValueError:
                pass
    logger.info("[BHAVCOPY] {} excluded days loaded.", len(excluded))

    run_bhavcopy_pipeline(
        bhavcopy_dir=args.bhavcopy_dir,
        out_dir=args.out_dir,
        start_date=args.start,
        end_date=args.end,
        excluded_days=excluded,
        overwrite=args.overwrite,
    )
