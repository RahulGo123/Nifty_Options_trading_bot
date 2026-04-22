"""
data/scrip_master.py
====================
Downloads the daily DhanHQ API Scrip Master CSV.
Extracts Security IDs, lot sizes, strike intervals, and expiries dynamically.
Runs once every morning at 08:45 AM.
"""

from asyncio import format_helpers
import aiohttp
import asyncio
import pandas as pd
import io
from datetime import date, datetime
from loguru import logger
from config.settings import settings
from config.contracts import NiftyWeeklyContract
from data.runtime_config import runtime_config, InstrumentConfig
from monitoring.alerting import send_alert
from persistence.write_buffer import write_buffer


class ScripMasterParser:
    """
    Downloads the daily DhanHQ Scrip Master CSV.
    Extracts security IDs, lot sizes, strike intervals,
    and expiry dates dynamically — nothing is hardcoded.
    """

    # Dhan's public daily scrip master URL
    SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

    # Minimum valid strike pairs before system halts
    MIN_STRIKE_PAIRS = 20

    def __init__(self):
        self._previous_lot_sizes = {}
        self._previous_weekdays = {}
        self._raw_df = None

    async def download(self, headers: dict = None) -> bool:
        """
        Downloads the Scrip Master CSV from Dhan.
        (Headers arg kept for interface compatibility with orchestrator).
        """
        logger.info("[SCRIP MASTER] Downloading daily Dhan Scrip Master CSV...")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.SCRIP_MASTER_URL,
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as response:
                    if response.status != 200:
                        logger.error(
                            f"[SCRIP MASTER] Download failed. HTTP {response.status}"
                        )
                        await send_alert(
                            f"CRITICAL: Dhan Scrip Master download failed — HTTP {response.status}"
                        )
                        return False

                    content = await response.read()
                    self._raw_df = pd.read_csv(io.BytesIO(content), low_memory=False)
                    logger.info(
                        f"[SCRIP MASTER] Downloaded successfully. "
                        f"{len(self._raw_df):,} rows loaded."
                    )
                    return True

        except Exception as e:
            logger.error(f"[SCRIP MASTER] Unexpected error: {e}")
            await send_alert(f"CRITICAL: Dhan Scrip Master unexpected error — {e}")
            return False

    def _get_nifty_options(self) -> pd.DataFrame:
        # Filters the raw Dhan CSV for Nifty 50 weekly options only.
        df = self._raw_df.copy()

        df.columns = df.columns.str.strip()
        df["SEM_EXM_EXCH_ID"] = df["SEM_EXM_EXCH_ID"].astype(str).str.strip()
        df["SEM_INSTRUMENT_NAME"] = df["SEM_INSTRUMENT_NAME"].astype(str).str.strip()
        df["SEM_CUSTOM_SYMBOL"] = df["SEM_CUSTOM_SYMBOL"].astype(str).str.strip()

        # FIX: Changed "NFO" to "NSE" based on Dhan's new CSV schema
        options = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_INSTRUMENT_NAME"] == "OPTIDX")
            & (df["SEM_CUSTOM_SYMBOL"].str.startswith("NIFTY"))
        ].copy()

        if options.empty:
            logger.error("[SCRIP MASTER] No NSE NIFTY OPTIDX rows found.")
            return pd.DataFrame()

        options["expiry_date"] = pd.to_datetime(
            options["SEM_EXPIRY_DATE"], errors="coerce"
        ).dt.date

        return options.dropna(subset=["expiry_date"])

    def _resolve_nearest_weekly_expiry(self, options: pd.DataFrame) -> date:
        today = date.today()
        future_expiries = sorted(
            [d for d in options["expiry_date"].unique() if d >= today]
        )

        if not future_expiries:
            logger.error("[SCRIP MASTER] No future expiry dates found in CSV.")
            return None

        nearest = future_expiries[0]
        weekday = nearest.strftime("%A")

        logger.info(f"[SCRIP MASTER] Nearest expiry resolved: {nearest} ({weekday})")

        prev_weekday = self._previous_weekdays.get("NIFTY")
        if prev_weekday and prev_weekday != weekday:
            msg = f"HIGH: NIFTY expiry weekday changed {prev_weekday} → {weekday}."
            logger.warning(f"[SCRIP MASTER] {msg}")
            asyncio.create_task(send_alert(msg))

        self._previous_weekdays["NIFTY"] = weekday
        return nearest

    def _resolve_lot_size(self, options: pd.DataFrame) -> int:
        if "SEM_LOT_SIZE" not in options.columns:
            return self._previous_lot_sizes.get("NIFTY", 65)

        lot_size = int(float(options["SEM_LOT_SIZE"].mode()[0]))

        prev_lot = self._previous_lot_sizes.get("NIFTY")
        if prev_lot and prev_lot != lot_size:
            msg = f"HIGH: NIFTY lot size changed {prev_lot} → {lot_size}."
            logger.warning(f"[SCRIP MASTER] {msg}")
            asyncio.create_task(send_alert(msg))

        self._previous_lot_sizes["NIFTY"] = lot_size
        logger.info(f"[SCRIP MASTER] Lot size resolved: {lot_size}")
        return lot_size

    def _resolve_strike_interval(self, options: pd.DataFrame, expiry: date) -> int:
        expiry_df = options[options["expiry_date"] == expiry].copy()

        # Dhan strike prices are floats like 23200.0000
        expiry_df["clean_strike"] = pd.to_numeric(
            expiry_df["SEM_STRIKE_PRICE"], errors="coerce"
        )
        strikes = sorted(expiry_df["clean_strike"].dropna().unique())

        if len(strikes) < 2:
            return 50

        gaps = [int(strikes[i + 1] - strikes[i]) for i in range(len(strikes) - 1)]
        interval = max(set(gaps), key=gaps.count)
        logger.info(f"[SCRIP MASTER] Strike interval derived: {interval} points")
        return interval

    def _build_token_map(
        self,
        options: pd.DataFrame,
        expiry: date,
        spot_approx: float,
        strike_interval: int,
        atm_range: int = 60,
    ) -> dict:
        contract = NiftyWeeklyContract()
        atm = contract.get_atm_strike(spot_approx, strike_interval)

        strikes_needed = [
            atm + (i * strike_interval) for i in range(-atm_range, atm_range + 1)
        ]

        logger.info(
            f"[SCRIP MASTER] Building Dhan token map — "
            f"ATM: {atm} | "
            f"Range: {atm - atm_range * strike_interval} to {atm + atm_range * strike_interval}"
        )

        expiry_options = options[options["expiry_date"] == expiry].copy()
        expiry_options["clean_strike"] = pd.to_numeric(
            expiry_options["SEM_STRIKE_PRICE"], errors="coerce"
        )

        token_map = {}
        for strike in strikes_needed:
            ce_row = expiry_options[
                (expiry_options["clean_strike"] == strike)
                & (expiry_options["SEM_OPTION_TYPE"].str.upper().str.contains("CE"))
            ]
            pe_row = expiry_options[
                (expiry_options["clean_strike"] == strike)
                & (expiry_options["SEM_OPTION_TYPE"].str.upper().str.contains("PE"))
            ]

            if not ce_row.empty and not pe_row.empty:
                token_map[strike] = {
                    "CE": str(ce_row["SEM_SMST_SECURITY_ID"].iloc[0]),
                    "PE": str(pe_row["SEM_SMST_SECURITY_ID"].iloc[0]),
                }

        logger.info(
            f"[SCRIP MASTER] Token map built: {len(token_map)} of {len(strikes_needed)} pairs resolved."
        )
        return token_map

    def _resolve_futures_token(self) -> str:
        """Resolves the current month Nifty Futures Security ID."""
        if self._raw_df is None:
            return None

        df = self._raw_df.copy()
        df.columns = df.columns.str.strip()
        df["SEM_EXM_EXCH_ID"] = df["SEM_EXM_EXCH_ID"].astype(str).str.strip()
        df["SEM_INSTRUMENT_NAME"] = df["SEM_INSTRUMENT_NAME"].astype(str).str.strip()
        df["SEM_CUSTOM_SYMBOL"] = df["SEM_CUSTOM_SYMBOL"].astype(str).str.strip()

        # FIX: Changed "NFO" to "NSE"
        futures = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE")
            & (df["SEM_INSTRUMENT_NAME"] == "FUTIDX")
            & (df["SEM_CUSTOM_SYMBOL"].str.startswith("NIFTY"))
        ].copy()

        if futures.empty:
            logger.error("[SCRIP MASTER] No NIFTY FUTIDX rows found.")
            return None

        futures["expiry_date"] = pd.to_datetime(
            futures["SEM_EXPIRY_DATE"], errors="coerce"
        ).dt.date

        today = date.today()
        future_expiries = futures[futures["expiry_date"] >= today].sort_values(
            "expiry_date"
        )

        if future_expiries.empty:
            logger.error("[SCRIP MASTER] No future Nifty Futures expiry found.")
            return None

        token = str(future_expiries["SEM_SMST_SECURITY_ID"].iloc[0])
        expiry = future_expiries["expiry_date"].iloc[0]
        logger.info(
            f"[SCRIP MASTER] Futures token resolved: {token} (expiry: {expiry})"
        )
        return token

    async def parse_and_load(
        self, headers: dict, spot_approx: float, account_balance: float
    ) -> bool:
        """
        Master method called at 08:45 AM.
        Downloads, parses, and loads everything into RuntimeConfig.
        """
        success = await self.download(headers)
        if not success:
            return False

        options = self._get_nifty_options()
        if options.empty:
            await send_alert(
                "CRITICAL: Scrip Master — no Nifty options found. System halted."
            )
            return False

        expiry = self._resolve_nearest_weekly_expiry(options)
        if not expiry:
            await send_alert(
                "CRITICAL: Scrip Master — cannot resolve expiry. System halted."
            )
            return False

        lot_size = self._resolve_lot_size(options)
        strike_interval = self._resolve_strike_interval(options, expiry)
        token_map = self._build_token_map(options, expiry, spot_approx, strike_interval)

        if len(token_map) < self.MIN_STRIKE_PAIRS:
            msg = f"CRITICAL: Token map has only {len(token_map)} pairs. System halted."
            logger.error(f"[SCRIP MASTER] {msg}")
            await send_alert(msg)
            return False

        futures_token = self._resolve_futures_token()

        instrument_config = InstrumentConfig(
            symbol="NIFTY",
            lot_size=lot_size,
            strike_interval=strike_interval,
            expiry_type="WEEKLY",
            next_expiry=expiry,
            expiry_weekday=expiry.strftime("%A"),
            instrument_tokens=token_map,
            futures_token=futures_token,
        )

        runtime_config.set_instrument(instrument_config)
        from config.constants import MAX_CONCURRENT_POSITIONS

        runtime_config.set_risk(
            total_capital=account_balance, max_concurrent=MAX_CONCURRENT_POSITIONS
        )

        # --- NEW: Persist to Database ---
        asyncio.create_task(
            write_buffer.put(
                "runtime_config",
                (
                    date.today(),
                    "NIFTY",
                    lot_size,
                    strike_interval,
                    "WEEKLY",
                    expiry,
                    expiry.strftime("%A"),
                    50.0,  # ivr_credit_entry (Default)
                    45.0,  # ivr_credit_exit (Default)
                    30.0,  # ivr_debit_entry (Default)
                    35.0,  # ivr_debit_exit (Default)
                    runtime_config.regime.pcr_bullish,
                    runtime_config.regime.pcr_bearish,
                    runtime_config.regime.atr_multiplier,
                ),
            )
        )

        logger.info("[SCRIP MASTER] Parse complete. System ready for market open.")
        return True


# Single instance
scrip_master_parser = ScripMasterParser()
