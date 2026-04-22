"""
backtest/dhan_data_fetcher.py
==============================
Downloads Nifty weekly options + futures 1-minute data from Dhan API.
Converts to weekly Parquet files matching the backtest engine schema.

NO token mapping files required. Uses Dhan's rolling options API
which handles strike lookup internally via ATM±N offsets.

Setup
-----
Add to .env:
    DHAN_ACCESS_TOKEN=your_token
    DHAN_CLIENT_ID=your_client_id

Run
---
    python backtest/dhan_data_fetcher.py --start 2023-01-01 --end 2024-12-31

Resume after interruption (skips already-fetched files):
    python backtest/dhan_data_fetcher.py --start 2023-01-01 --end 2024-12-31

Force re-download:
    python backtest/dhan_data_fetcher.py --start 2023-01-01 --end 2024-12-31 --overwrite

Runtime: ~2-3 hours for 2 years of data. Run overnight.
Dhan covers last 5 years (~March 2021 onwards as of March 2026).

API limits
----------
- Rolling options: 30 days per call
- Futures intraday: 90 days per call
- Rate limit: ~10 req/sec (script uses 0.2s delay = safe)
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from decouple import config
from loguru import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DHAN_BASE_URL = "https://api.dhan.co/v2"
NIFTY_SECURITY_ID = "13"  # Nifty 50 index (for rolling options)
NIFTY_FUT_ID = "13"  # Nifty Futures nearest month
NIFTY_SPOT_ID = "13"  # Nifty spot index

STRIKE_INTERVAL = 50
ATM_RANGE = 10  # ATM±10 — Dhan's maximum for index options

OPTIONS_WINDOW_DAYS = 28  # Dhan allows 30; use 28 for safety
FUTURES_WINDOW_DAYS = 88  # Dhan allows 90; use 88 for safety
RATE_LIMIT_DELAY = 0.25  # seconds between calls

DEFAULT_OUT_DIR = Path("data_store/parquet")
DEFAULT_CACHE_DIR = Path("data_store/raw_cache/dhan")


# ---------------------------------------------------------------------------
# Dhan API client
# ---------------------------------------------------------------------------


class DhanClient:

    def __init__(self, access_token: str, client_id: str):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "access-token": access_token,
                "client-id": client_id,
            }
        )
        self.call_count = 0

    def _post(self, endpoint: str, payload: dict, retries: int = 3) -> Optional[dict]:
        url = f"{DHAN_BASE_URL}{endpoint}"
        for attempt in range(1, retries + 1):
            try:
                time.sleep(RATE_LIMIT_DELAY)
                resp = self.session.post(url, json=payload, timeout=30)
                self.call_count += 1

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    wait = 10 * attempt
                    logger.warning("[DHAN] Rate limited — waiting {}s", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code in (401, 403):
                    logger.error(
                        "[DHAN] Auth error {}. "
                        "Check DHAN_ACCESS_TOKEN and that your ₹499 "
                        "data subscription is active.",
                        resp.status_code,
                    )
                    return None

                logger.warning(
                    "[DHAN] HTTP {} attempt {}/{}: {}",
                    resp.status_code,
                    attempt,
                    retries,
                    resp.text[:150],
                )

            except requests.RequestException as exc:
                logger.warning(
                    "[DHAN] Network error attempt {}/{}: {}", attempt, retries, exc
                )
                if attempt < retries:
                    time.sleep(3 * attempt)

        return None

    def fetch_rolling_option(
        self,
        strike_offset: str,  # "ATM", "ATM+1", "ATM-1", ..., "ATM+10", "ATM-10"
        option_type: str,  # "CALL" or "PUT"
        from_date: str,  # "YYYY-MM-DD"
        to_date: str,  # "YYYY-MM-DD"
    ) -> Optional[dict]:
        """
        Fetches 1-min expired options data for one ATM-relative strike.
        Returns the full response dict (contains 'data' key with 'ce'/'pe').
        """
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval": "1",
            "securityId": NIFTY_SECURITY_ID,
            "instrument": "OPTIDX",
            "expiryFlag": "WEEK",
            "expiryCode": 1,  # nearest weekly expiry from fromDate
            "strike": strike_offset,
            "drvOptionType": option_type,
            "requiredData": [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
                "iv",
                "spot",
                "strike",
            ],
            "fromDate": from_date,
            "toDate": to_date,
        }
        return self._post("/charts/rollingoption", payload)

    def fetch_futures(self, from_date: str, to_date: str) -> Optional[dict]:
        payload = {
            "securityId": "13",
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "interval": 1,
            "oi": False,
            "fromDate": f"{from_date} 09:15:00",
            "toDate": f"{to_date} 15:30:00",
        }
        return self._post("/charts/intraday", payload)

    def fetch_spot(self, from_date: str, to_date: str) -> Optional[dict]:
        """Fetches 1-min Nifty Spot OHLC."""
        payload = {
            "securityId": NIFTY_SPOT_ID,
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "interval": "1",
            "oi": False,
            "fromDate": f"{from_date} 09:15:00",
            "toDate": f"{to_date} 15:30:00",
        }
        return self._post("/charts/intraday", payload)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _to_ist(timestamps: list) -> pd.DatetimeIndex:
    return (
        pd.to_datetime(timestamps, unit="s", utc=True)
        .tz_convert("Asia/Kolkata")
        .tz_localize(None)
    )


def parse_intraday(data: dict) -> pd.DataFrame:
    """Parses /charts/intraday response (futures or spot)."""
    if not data or not data.get("timestamp"):
        return pd.DataFrame()

    ts = data["timestamp"]
    n = len(ts)

    def pad(k, default=0.0):
        v = data.get(k)
        return v if v and len(v) == n else [default] * n

    df = pd.DataFrame(
        {
            "open": pad("open"),
            "high": pad("high"),
            "low": pad("low"),
            "close": pad("close"),
            "volume": pad("volume", 0),
            "oi": pad("open_interest", 0),
        },
        index=_to_ist(ts),
    )

    df.index.name = "timestamp"
    df = df[df["close"] > 0].sort_index()
    return df


def parse_rolling_option(response: dict, option_type: str) -> pd.DataFrame:
    """
    Parses /charts/rollingoption response for one side (CE or PE).
    Returns DataFrame with columns: open, high, low, close, volume, oi, iv, spot, strike.
    """
    if not response:
        return pd.DataFrame()

    data = response.get("data", {})
    key = "ce" if option_type == "CALL" else "pe"
    side = data.get(key) if data else None

    if not side or not side.get("timestamp"):
        return pd.DataFrame()

    ts = side["timestamp"]
    n = len(ts)

    def pad(k, default=0.0):
        v = side.get(k)
        return v if v and len(v) == n else [default] * n

    df = pd.DataFrame(
        {
            "open": pad("open"),
            "high": pad("high"),
            "low": pad("low"),
            "close": pad("close"),
            "volume": pad("volume", 0),
            "oi": pad("oi", 0),
            "iv": pad("iv", 0.0),
            "spot": pad("spot", 0.0),
            "strike": pad("strike", 0),
        },
        index=_to_ist(ts),
    )

    df.index.name = "timestamp"
    df = df[df["close"] > 0].sort_index()
    return df


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


def date_windows(start: date, end: date, window_days: int) -> List[Tuple[date, date]]:
    windows, cursor = [], start
    while cursor <= end:
        windows.append((cursor, min(cursor + timedelta(days=window_days - 1), end)))
        cursor += timedelta(days=window_days)
    return windows


def thursday_expiries(start: date, end: date) -> List[date]:
    """All Thursdays in range — Nifty weekly expiry day (2021-2024)."""
    d = start
    while d.weekday() != 3:  # 3 = Thursday
        d += timedelta(days=1)
    expiries = []
    while d <= end:
        expiries.append(d)
        d += timedelta(days=7)
    return expiries


# ---------------------------------------------------------------------------
# Episode builder
# ---------------------------------------------------------------------------


def build_episode(
    expiry: date,
    options_data: Dict[Tuple[int, str], pd.DataFrame],
    futures_df: pd.DataFrame,
    spot_df: pd.DataFrame,
    excluded_days: set,
) -> pd.DataFrame:
    """
    Assembles one weekly episode matching the TradingTuitions Parquet schema.

    Index: DatetimeIndex (1-minute bars, 09:15–15:30)
    Columns: open, high, low, close, volume, oi,
             ticker, expiry, trading_date, symbol, instrument,
             strike, option_type
    """
    # Episode spans Mon–Thu of expiry week
    ep_start = expiry - timedelta(days=expiry.weekday())  # Monday
    ep_end = expiry

    all_rows = []

    # ── Futures ──
    for ts, row in futures_df.iterrows():
        td = ts.date()
        if ep_start <= td <= ep_end and td not in excluded_days:
            all_rows.append(
                {
                    "datetime": ts,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "oi": 0,
                    "ticker": "NIFTY-FUT",
                    "expiry": expiry,
                    "trading_date": td,
                    "symbol": "NIFTY",
                    "instrument": "FUTURES",
                    "strike": None,
                    "option_type": None,
                }
            )

    # ── Spot ──
    for ts, row in spot_df.iterrows():
        td = ts.date()
        if ep_start <= td <= ep_end and td not in excluded_days:
            all_rows.append(
                {
                    "datetime": ts,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": 1,
                    "oi": 0,
                    "ticker": "NIFTY",
                    "expiry": expiry,
                    "trading_date": td,
                    "symbol": "NIFTY",
                    "instrument": "SPOT",
                    "strike": None,
                    "option_type": None,
                }
            )

    # ── Options + VIX proxy from IV ──
    # Collect per-timestamp ATM IV for VIX bar
    atm_iv_by_ts: Dict[pd.Timestamp, list] = {}

    for (strike, otype), df in options_data.items():
        opt_str = "CE" if otype == "CALL" else "PE"
        ticker = f"NIFTYWK{strike}{opt_str}"

        for ts, row in df.iterrows():
            td = ts.date()
            if not (ep_start <= td <= ep_end and td not in excluded_days):
                continue
            if row.close <= 0:
                continue

            all_rows.append(
                {
                    "datetime": ts,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                    "oi": row.oi,
                    "ticker": ticker,
                    "expiry": expiry,
                    "trading_date": td,
                    "symbol": "NIFTY",
                    "instrument": "OPTION",
                    "strike": strike,
                    "option_type": opt_str,
                }
            )

            # Collect CE IV for VIX proxy (CE IV is standard for INDIAVIX calc)
            if opt_str == "CE" and row.iv > 0:
                spot = row.spot if row.spot > 0 else 0
                atm = round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL if spot > 0 else 0
                atm_iv_by_ts.setdefault(ts, []).append((abs(strike - atm), row.iv))

    # ── VIX proxy — use CE IV of strike closest to ATM ──
    for ts, iv_list in atm_iv_by_ts.items():
        td = ts.date()
        if not (ep_start <= td <= ep_end and td not in excluded_days):
            continue
        iv_list.sort(key=lambda x: x[0])  # sort by distance from ATM
        atm_iv = iv_list[0][1]  # IV of closest strike
        # Dhan returns IV as % already (e.g. 18.5 = 18.5%)
        # TradingTuitions VIX format is also in % (INDIAVIX × 100 / 100 = raw)
        # The engine divides by 100 to get decimal. So write as-is.
        all_rows.append(
            {
                "datetime": ts,
                "open": atm_iv,
                "high": atm_iv,
                "low": atm_iv,
                "close": atm_iv,
                "volume": 0,
                "oi": 0,
                "ticker": "INDIAVIX",
                "expiry": expiry,
                "trading_date": td,
                "symbol": "INDIAVIX",
                "instrument": "VIX",
                "strike": None,
                "option_type": None,
            }
        )

    if not all_rows:
        return pd.DataFrame()

    df_out = (
        pd.DataFrame(all_rows)
        .set_index("datetime")
        .sort_index()
        .between_time("09:15", "15:30")
    )

    if df_out["trading_date"].nunique() < 2:
        return pd.DataFrame()

    return df_out


# ---------------------------------------------------------------------------
# SMA
# ---------------------------------------------------------------------------


def build_sma(futures_chunks: List[pd.DataFrame], out_dir: Path) -> None:
    if not futures_chunks:
        return
    all_fut = pd.concat(futures_chunks).sort_index()
    daily = all_fut["close"].groupby(all_fut.index.date).last()
    daily.index = pd.to_datetime(daily.index)
    sma_df = daily.to_frame("close")
    sma_df["sma20"] = sma_df["close"].rolling(20, min_periods=1).mean()
    out_path = out_dir / "nifty_fut_sma20.parquet"
    sma_df[["sma20"]].to_parquet(out_path, engine="pyarrow", compression="snappy")
    logger.info(
        "[DHAN] SMA20 written: {} days ({} to {})",
        len(sma_df),
        sma_df.index[0].date(),
        sma_df.index[-1].date(),
    )


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_fetch(
    client: DhanClient,
    start: date,
    end: date,
    out_dir: Path,
    cache_dir: Path,
    excluded_days: set,
    overwrite: bool = False,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Fetch Futures and Spot ──
    logger.info("[DHAN] Fetching Nifty Futures and Spot ({} to {})...", start, end)
    futures_chunks, spot_chunks = [], []

    for w_start, w_end in date_windows(start, end, FUTURES_WINDOW_DAYS):
        # Futures
        cache = cache_dir / f"fut_{w_start}_{w_end}.parquet"
        if cache.exists() and not overwrite:
            futures_chunks.append(pd.read_parquet(cache))
        else:
            raw = client.fetch_futures(str(w_start), str(w_end))
            df = parse_intraday(raw)
            if not df.empty:
                df.to_parquet(cache, engine="pyarrow", compression="snappy")
                futures_chunks.append(df)
                logger.info(
                    "[DHAN] Futures {} to {}: {:,} bars", w_start, w_end, len(df)
                )
            else:
                logger.warning("[DHAN] No futures data {} to {}", w_start, w_end)

        # Spot
        cache = cache_dir / f"spot_{w_start}_{w_end}.parquet"
        if cache.exists() and not overwrite:
            spot_chunks.append(pd.read_parquet(cache))
        else:
            raw = client.fetch_spot(str(w_start), str(w_end))
            df = parse_intraday(raw)
            if not df.empty:
                df.to_parquet(cache, engine="pyarrow", compression="snappy")
                spot_chunks.append(df)

    futures_df = (
        pd.concat(futures_chunks).sort_index() if futures_chunks else pd.DataFrame()
    )
    spot_df = pd.concat(spot_chunks).sort_index() if spot_chunks else pd.DataFrame()

    if futures_chunks:
        build_sma(futures_chunks, out_dir)

    logger.info(
        "[DHAN] Futures: {:,} bars | Spot: {:,} bars", len(futures_df), len(spot_df)
    )

    # ── Step 2: Fetch Options, episode by episode ──
    expiries = thursday_expiries(start, end)
    logger.info("[DHAN] {} weekly expiries to process.", len(expiries))

    # Strike offsets: ATM, ATM+1..ATM+10, ATM-1..ATM-10
    offsets = (
        ["ATM"]
        + [f"ATM+{i}" for i in range(1, ATM_RANGE + 1)]
        + [f"ATM-{i}" for i in range(1, ATM_RANGE + 1)]
    )

    written = skipped = failed = 0

    for expiry in expiries:
        out_path = out_dir / f"weekly_{expiry}_dhan.parquet"

        if out_path.exists() and not overwrite:
            skipped += 1
            continue

        # Fetch window: full week before expiry through expiry day
        # Using OPTIONS_WINDOW_DAYS chunk starting from Monday of expiry week
        ep_monday = expiry - timedelta(days=expiry.weekday())
        fetch_from = ep_monday
        fetch_to = min(expiry + timedelta(days=1), end + timedelta(days=1))

        # Clamp
        fetch_from = max(fetch_from, start)

        logger.info(
            "[DHAN] Expiry {} (fetch {} to {})...", expiry, fetch_from, fetch_to
        )

        options_data: Dict[Tuple[int, str], pd.DataFrame] = {}

        for offset in offsets:
            for otype in ["CALL", "PUT"]:
                cache = cache_dir / f"opt_{expiry}_{offset}_{otype}.parquet"

                if cache.exists() and not overwrite:
                    df = pd.read_parquet(cache)
                else:
                    raw = client.fetch_rolling_option(
                        strike_offset=offset,
                        option_type=otype,
                        from_date=str(fetch_from),
                        to_date=str(fetch_to),
                    )
                    df = parse_rolling_option(raw, otype)
                    if not df.empty:
                        df.to_parquet(cache, engine="pyarrow", compression="snappy")

                if df.empty:
                    continue

                # Extract actual strike from the 'strike' column
                # (Dhan fills this with the real strike price)
                if "strike" in df.columns and df["strike"].max() > 0:
                    actual_strike = int(df["strike"].mode().iloc[0])
                    key = (actual_strike, otype)
                    if key in options_data:
                        # Merge, keep later rows on duplicate timestamps
                        options_data[key] = (
                            pd.concat([options_data[key], df]).groupby(level=0).last()
                        )
                    else:
                        options_data[key] = df

        if not options_data:
            logger.warning("[DHAN] No options data for expiry {} — skipping.", expiry)
            failed += 1
            continue

        # Slice futures and spot to this episode week
        ep_start = expiry - timedelta(days=expiry.weekday())  # Monday
        ep_fut = (
            futures_df[
                (futures_df.index.date >= ep_start) & (futures_df.index.date <= expiry)
            ]
            if not futures_df.empty
            else pd.DataFrame()
        )

        ep_spot = (
            spot_df[(spot_df.index.date >= ep_start) & (spot_df.index.date <= expiry)]
            if not spot_df.empty
            else pd.DataFrame()
        )

        df_ep = build_episode(expiry, options_data, ep_fut, ep_spot, excluded_days)

        if df_ep.empty:
            logger.warning("[DHAN] Empty episode for {} — skipping.", expiry)
            failed += 1
            continue

        df_ep.to_parquet(out_path, engine="pyarrow", compression="snappy", index=True)
        written += 1

        n_days = df_ep["trading_date"].nunique()
        n_strikes = df_ep[df_ep["instrument"] == "OPTION"]["ticker"].nunique()
        logger.info(
            "[DHAN] Written: {} | {} days | {} strikes | {:,} bars",
            out_path.name,
            n_days,
            n_strikes,
            len(df_ep),
        )

    logger.info(
        "[DHAN] Done. Written: {} | Skipped: {} | Failed: {} | API calls: {}",
        written,
        skipped,
        failed,
        client.call_count,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Nifty weekly options history from Dhan API"
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(2023, 1, 1),
        help="Start date. Dhan covers last 5 years (~Mar 2021 from Mar 2026).",
    )
    parser.add_argument("--end", type=date.fromisoformat, default=date(2024, 12, 31))
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument(
        "--excluded", type=Path, default=Path("data_store/parquet/excluded_days.txt")
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    access_token = config("DHAN_ACCESS_TOKEN", default="")
    client_id = config("DHAN_CLIENT_ID", default="")

    if not access_token or not client_id:
        logger.error(
            "[DHAN] Missing credentials.\n"
            "Add to .env file:\n"
            "  DHAN_ACCESS_TOKEN=your_token\n"
            "  DHAN_CLIENT_ID=your_client_id\n"
            "Get your access token from: https://dhanhq.co/docs/v2/authentication/"
        )
        sys.exit(1)

    excluded: set = set()
    if args.excluded.exists():
        for line in args.excluded.read_text(encoding="utf-8").strip().splitlines():
            try:
                excluded.add(date.fromisoformat(line.strip()))
            except ValueError:
                pass
    logger.info("[DHAN] {} excluded days loaded.", len(excluded))

    # Estimate runtime
    n_expiries = len(thursday_expiries(args.start, args.end))
    n_offsets = ATM_RANGE * 2 + 1  # 21 strike offsets
    n_calls = n_expiries * n_offsets * 2  # × 2 for CE and PE
    est_min = n_calls * RATE_LIMIT_DELAY / 60
    logger.info(
        "[DHAN] {} expiries × {} offsets × 2 sides = ~{} API calls (~{:.0f} min)",
        n_expiries,
        n_offsets,
        n_calls,
        est_min,
    )

    client = DhanClient(access_token=access_token, client_id=client_id)

    run_fetch(
        client=client,
        start=args.start,
        end=args.end,
        out_dir=args.out_dir,
        cache_dir=args.cache_dir,
        excluded_days=excluded,
        overwrite=args.overwrite,
    )
