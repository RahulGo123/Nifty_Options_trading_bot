"""
scheduler/market_scheduler.py
==============================
APScheduler-based daily timeline orchestrator.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from decouple import config

from config.constants import (
    WATCHDOG_INTERVAL,
    PCR_ACTIVATION_TIME,
    IVR_UPDATE_INTERVAL,
    OI_WALL_UPDATE_INTERVAL,
)
from monitoring.alerting import send_alert
from execution.margin_cache import margin_cache

SIGNAL_EVAL_INTERVAL: int = 1


class MarketScheduler:
    def __init__(self):
        self._scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
        self._signal_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._pcr_active: bool = False
        self._market_open: bool = False
        self._session_ended: bool = False
        self._morning_fired: bool = False
        self._afternoon_fired: bool = False
        self._prev_oi_data: dict = {}  # For change-in-OI (PCR)

        self.heartbeat_counter = None
        self.shutdown_event = None
        self.auth_engine = None
        self.ws_manager = None
        self.lifecycle_manager = None
        self.worker_process = None
        self._report_sent: bool = False
        self._premarket_done = False

        # --- NEW: EOD Reporting Accumulators ---
        from monitoring.reporter import RegimeLog

        self.regime_log = RegimeLog()
        # ---------------------------------------

    def start(self) -> None:
        self._register_jobs()
        self._scheduler.start()
        logger.info("[SCHEDULER] APScheduler started. All jobs registered.")

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        logger.info("[SCHEDULER] APScheduler stopped.")

    def _register_jobs(self) -> None:
        live = config("LIVE_TRADING", default=False, cast=bool)
        mode = "LIVE" if live else "GHOST"
        logger.info("[SCHEDULER] Registering jobs for {} mode.", mode)

        self._scheduler.add_job(
            self._job_premarket_init,
            CronTrigger(hour=8, minute=45, timezone="Asia/Kolkata"),
            id="premarket_init",
            name="Pre-Market Init",
            max_instances=1,
            misfire_grace_time=120,
        )

        self._scheduler.add_job(
            self._job_market_open,
            CronTrigger(hour=9, minute=15, timezone="Asia/Kolkata"),
            id="market_open",
            name="Market Open",
            max_instances=1,
            misfire_grace_time=60,
        )

        self._scheduler.add_job(
            self._job_refresh_margin,
            CronTrigger(
                hour=PCR_ACTIVATION_TIME.hour,
                minute=(PCR_ACTIVATION_TIME.minute - 1) % 60,
                timezone="Asia/Kolkata",
            ),
            id="pre_entry_refresh_margin",
            name=f"Pre-Entry Margin Refresh ({PCR_ACTIVATION_TIME.hour}:{PCR_ACTIVATION_TIME.minute - 1})",
            max_instances=1,
            misfire_grace_time=60,
        )

        self._scheduler.add_job(
            self._job_activate_pcr,
            CronTrigger(
                hour=PCR_ACTIVATION_TIME.hour,
                minute=PCR_ACTIVATION_TIME.minute,
                timezone="Asia/Kolkata",
            ),
            id="activate_pcr",
            name="Activate PCR Engine",
            max_instances=1,
            misfire_grace_time=120,
        )

        self._scheduler.add_job(
            self._job_ivr_update,
            CronTrigger(hour="9-15", minute="*/5", timezone="Asia/Kolkata"),
            id="ivr_update",
            name="IVR Update",
            max_instances=1,
            misfire_grace_time=60,
        )

        self._scheduler.add_job(
            self._job_oi_wall_update,
            CronTrigger(hour="9-15", minute="*/5", timezone="Asia/Kolkata"),
            id="oi_wall_update",
            name="OI Wall Update",
            max_instances=1,
            misfire_grace_time=60,
        )

        self._scheduler.add_job(
            self._job_reconcile_positions,
            CronTrigger(hour="9-15", second="0", timezone="Asia/Kolkata"),
            id="reconcile_positions",
            name="Broker Position Reconciliation",
            max_instances=1,
            misfire_grace_time=10,
        )

        # Greeks update every 60 seconds for live risk monitoring
        self._scheduler.add_job(
            self._job_update_live_greeks,
            CronTrigger(hour="9-15", second="30", timezone="Asia/Kolkata"),
            id="greeks_update",
            name="Live Greeks Calculation",
            max_instances=1,
            misfire_grace_time=10,
        )

        # Margin refresh every 15 minutes (dynamic volatility adjustment)
        self._scheduler.add_job(
            self._job_refresh_margin,
            CronTrigger(
                hour="9-15",
                minute="0,15,30,45",
                timezone="Asia/Kolkata",
            ),
            id="refresh_margin",
            name="Margin Requirement Refresh",
            max_instances=1,
            misfire_grace_time=60,
        )

        # Theta Decay Harvester every 30 minutes
        self._scheduler.add_job(
            self._job_theta_harvest,
            "interval",
            seconds=60,  # matches backtest: _check_exits runs on every 1-min bar
            id="theta_harvest",
            name="Theta Decay Harvester",
            max_instances=1,
            misfire_grace_time=30,
        )

        self._scheduler.add_job(
            self._job_update_macro_sma,
            CronTrigger(hour=15, minute=10, timezone="Asia/Kolkata"),
            id="update_macro_sma",
            name="Update 20-day Macro SMA",
            max_instances=1,
        )

        self._scheduler.add_job(
            self._job_refresh_margin,
            CronTrigger(hour=15, minute=8, timezone="Asia/Kolkata"),
            id="eod_refresh_margin",
            name="EOD Margin Refresh",
            max_instances=1,
            misfire_grace_time=60,
        )

        self._scheduler.add_job(
            self._job_eod_reporting_and_kill,
            CronTrigger(hour=15, minute=10, timezone="Asia/Kolkata"),
            id="eod_report",
            name="EOD Reporting & Expiry Kill 03:10 PM",
            max_instances=1,
        )

        self._scheduler.add_job(
            self._job_final_shutdown,
            CronTrigger(hour=15, minute=30, timezone="Asia/Kolkata"),
            id="final_shutdown",
            name="Final Shutdown 03:30 PM",
            max_instances=1,
        )

        self._scheduler.add_job(
            self._job_heartbeat,
            "interval",
            seconds=WATCHDOG_INTERVAL,
            id="watchdog_heartbeat",
            name="Watchdog Heartbeat",
            max_instances=1,
        )

        # 1-minute job for precise regime tracking and state sync
        self._scheduler.add_job(
            self._job_minute_stats,
            CronTrigger(hour="9-15", minute="*", timezone="Asia/Kolkata"),
            id="minute_stats",
            name="Minute Stats Tracking",
            max_instances=1,
        )
        logger.info("[SCHEDULER] {} jobs registered.", len(self._scheduler.get_jobs()))

    async def _job_premarket_init(self) -> None:
        if self._premarket_done:
            logger.info("[SCHEDULER] Pre-market init already done. Skipping duplicate.")
            return
        self._premarket_done = True
        logger.info("[SCHEDULER] ⏰ 08:45 — Pre-market init starting...")

        from auth.dhan_auth import auth_engine
        from data.scrip_master import scrip_master_parser
        from analytics.ivr_engine import ivr_engine
        from macro.event_calendar import event_calendar
        from analytics.rate_validator import rate_validator
        from persistence.database import fetch
        from data.runtime_config import runtime_config
        from config.settings import settings

        logger.info("[SCHEDULER] Step 1/5: Authentication...")
        auth_engine.ensure_valid_token()
        ok = auth_engine.is_authenticated()
        if not ok:
            msg = "CRITICAL: Authentication failed at 08:45. System halted."
            logger.error("[SCHEDULER] {}", msg)
            await send_alert(msg)
            return

        self.auth_engine = auth_engine

        # Sync real Dhan balance to disk before session initializes
        from utils.live_sync import sync_balance_from_dhan
        from config.settings import settings

        await sync_balance_from_dhan(auth_engine, settings, is_premarket=True)

        logger.info("[SCHEDULER] Step 2/5: Scrip Master parse...")
        headers = auth_engine.get_headers()
        spot_approx = await self._fetch_spot_approximation()

        if spot_approx is None:
            # THE RUTHLESS HALT: No price means we cannot resolve strikes.
            msg = "CRITICAL: No valid price seed found. Scrip Master cannot initialize."
            logger.error(f"[SCHEDULER] {msg}")
            await send_alert(msg)
            return

        # --- NEW: PERSISTENT CAPITAL LOAD ---
        import json
        import os

        account_balance = settings.total_capital
        state_path = os.path.join("data", "capital_state.json")
        if os.path.exists(state_path):
            try:
                with open(state_path, "r") as f:
                    state = json.load(f)
                    # Check if the state is from a previous day. If so, reset the fired flags.
                    last_updated_str = state.get("last_updated", "")
                    if last_updated_str:
                        last_date = datetime.fromisoformat(last_updated_str).date()
                        if last_date < date.today():
                            logger.info(
                                "[SCHEDULER] New day detected. Resetting session flags."
                            )
                            self._morning_fired = False
                            self._afternoon_fired = False
                        else:
                            self._morning_fired = state.get("morning_fired", False)
                            self._afternoon_fired = state.get("afternoon_fired", False)

                    account_balance = state.get("final_balance", account_balance)

                logger.info(
                    f"[SCHEDULER] Loaded persistent capital: ₹{account_balance:,.2f} | Morning: {self._morning_fired} | Afternoon: {self._afternoon_fired}"
                )
            except Exception as e:
                logger.warning(
                    f"[SCHEDULER] Failed to load persistent capital: {e}. Using .env default."
                )
        # ------------------------------------

        ok = await scrip_master_parser.parse_and_load(
            headers=headers,
            spot_approx=spot_approx,
            account_balance=account_balance,
        )
        if not ok:
            msg = "CRITICAL: Scrip Master parse failed at 08:45. System halted."
            logger.error("[SCHEDULER] {}", msg)
            await send_alert(msg)
            return

        logger.info(
            "[SCHEDULER] Step 2.5/5: Margin Cache deferred to 09:15 open (requires live Spot)..."
        )

        # --- NEW: Step 2.8: Gap Tracker Bootstrap ---
        logger.info("[SCHEDULER] Step 2.8/5: Gap Tracker bootstrap...")
        try:
            from analytics.gap_tracker import gap_tracker

            # Initializes the tracker with yesterday's close.
            gap_tracker.initialize_previous_close(spot_approx)
        except Exception as exc:
            logger.warning(f"[SCHEDULER] Gap Tracker bootstrap failed: {exc}")
        # ---------------------------------------------

        logger.info("[SCHEDULER] Step 3/5: Volatility Engine bootstrap...")
        try:
            # Fetch last 252 trading days of VIX close from DB
            rows = await fetch(
                "SELECT iv_close FROM iv_history WHERE symbol = 'NIFTY' ORDER BY date DESC LIMIT 252"
            )
            # Database returns newest first (DESC), IVREngine expects oldest first
            vix_history = [
                float(r["iv_close"])
                for r in reversed(rows)
                if r["iv_close"] is not None
            ]
            ivr_engine.bootstrap(vix_history)
        except Exception as exc:
            logger.warning(
                f"[SCHEDULER] IVR bootstrap failed: {exc}. Initializing with empty history."
            )
            ivr_engine.bootstrap([])

        logger.info("[SCHEDULER] Step 4/5: Macro event calendar fetch...")
        await event_calendar.fetch_and_classify(date.today())
        event_calendar.load_into_pre_execution_gate()

        logger.info("[SCHEDULER] Step 5/5: Rate validation...")
        await rate_validator.run()

        if not runtime_config.validate():
            msg = "CRITICAL: RuntimeConfig validation failed. System halted."
            logger.error("[SCHEDULER] {}", msg)
            await send_alert(msg)
            return

        logger.info("[SCHEDULER] ✅ Pre-market init complete. System ready.")
        await send_alert(
            f"INFO: Pre-market init complete. "
            f"Expiry: {runtime_config.instruments['NIFTY'].next_expiry} | "
            f"Lot size: {runtime_config.instruments['NIFTY'].lot_size} | "
            f"Tier-1 events today: {event_calendar.tier1_count}"
        )

    async def _seed_market_data_snapshot(self, nifty) -> None:
        """
        Solves the WebSocket Cold Start problem.
        Fetches the current REST API snapshot and seeds the memory bank.
        """
        import aiohttp
        from data.market_data_store import market_data_store
        from loguru import logger

        logger.info("[SCHEDULER] Seeding market data store via REST API snapshot...")

        try:
            headers = self.auth_engine.get_headers()
            url = "https://api.dhan.co/v2/marketfeed/quote"

            # 1. Extract all token IDs for the mapped strikes
            tokens = []
            token_to_strike_map = (
                {}
            )  # Needed to map Dhan's response back to our strikes

            for strike, options in nifty.instrument_tokens.items():
                if "CE" in options:
                    token_str = str(options["CE"])
                    tokens.append(int(token_str))
                    token_to_strike_map[token_str] = {"strike": strike, "type": "CE"}
                if "PE" in options:
                    token_str = str(options["PE"])
                    tokens.append(int(token_str))
                    token_to_strike_map[token_str] = {"strike": strike, "type": "PE"}

            # ALSO seed the Futures token so _compute_atm doesn't fail at open
            fut_token_str = str(nifty.futures_token)
            tokens.append(int(fut_token_str))
            token_to_strike_map[fut_token_str] = {
                "strike": nifty.futures_token,
                "type": "FUT",
            }

            # 2. Format the payload for Dhan's API
            payload = {
                "NSE_FNO": tokens,
                "NSE_IDX": [21],  # Add India VIX (Dhan ID: 21)
            }

            # 3. Fire the asynchronous REST request
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status != 200:
                        logger.error(
                            f"[SCHEDULER] Snapshot API failed. HTTP Status: {response.status}"
                        )
                        return

                    data = await response.json()

            # 4. Parse the response and inject it into the memory bank
            # 4. Parse the response with the correct Dhan v2 keys
            count = 0
            quotes = data.get("data", {}).get("NSE_FNO", {})

            for token_str, quote_data in quotes.items():
                mapping = token_to_strike_map.get(token_str)
                if not mapping:
                    continue

                # DHAN V2 KEY FIX: Use 'last_price' as fallback if bid/ask are 0
                best_bid = quote_data.get("buy_price", 0.0)
                best_ask = quote_data.get("sell_price", 0.0)
                lp = quote_data.get("last_price", 0.0)

                # If the orderbook is thin (common in REST snapshots), use LP to avoid 0.0
                if best_bid == 0 or best_ask == 0:
                    best_bid = best_bid or lp
                    best_ask = best_ask or lp

                market_data_store.update(
                    strike=mapping["strike"],
                    option_type=mapping["type"],
                    bid=best_bid,
                    ask=best_ask,
                    oi=quote_data.get("OI", 0),
                )

                # Log the first quote to verify the data pipe is "hot"
                if count == 0:
                    logger.info(
                        f"[DEBUG] Raw Snapshot Sample: Strike {mapping['strike']} {mapping['type']} | Bid: {best_bid} | Ask: {best_ask}"
                    )

                count += 1

            logger.info(
                f"[SCHEDULER] Market data memory bank successfully seeded with {count} quotes."
            )

            # 5. Extract VIX if present
            indices = data.get("data", {}).get("NSE_IDX", {})
            vix_data = indices.get("21")  # India VIX (Dhan ID: 21)
            if vix_data:
                vix_lp = vix_data.get("last_price", 0.0)
                if vix_lp > 0:
                    from analytics.ivr_engine import ivr_engine

                    ivr_engine.update(vix_lp, datetime.now())
                    logger.info(f"[SCHEDULER] VIX seeded from REST snapshot: {vix_lp}")

            # --- NEW: Trigger immediate Greeks refresh on Hot Boot ---
            try:
                await self._job_update_live_greeks()
                logger.info("[MAIN] Hot Boot: Initial Greeks calculated.")
            except Exception as e:
                logger.debug(
                    f"[MAIN] Hot Boot Greeks failed (normal if no positions): {e}"
                )

        except Exception as exc:
            logger.error(f"[SCHEDULER] Snapshot seeding failed: {exc}")

    async def _job_update_live_greeks(self) -> None:
        """
        Sweeps open positions and updates their Greeks (Delta, Theta, etc)
        using the vectorized GreeksEngine.
        """
        from risk.portfolio_state import portfolio_state

        if portfolio_state.position_count == 0:
            return

        from analytics.greeks_engine import greeks_engine
        from data.market_data_store import market_data_store
        from data.runtime_config import runtime_config
        import numpy as np
        from datetime import datetime, time

        try:
            # 1. Prepare Market Context
            nifty = runtime_config.instruments.get("NIFTY")
            if not nifty:
                return

            from analytics.vwap_engine import vwap_engine

            spot = vwap_engine.last_price
            if spot <= 0:
                return

            # Calculate time to expiry in years
            now = datetime.now()
            expiry_dt = datetime.combine(nifty.next_expiry, time(15, 30))
            time_to_expiry = (expiry_dt - now).total_seconds() / (365 * 24 * 3600)
            if time_to_expiry <= 0:
                return

            # 2. Process each position
            for pid, pos in portfolio_state.open_positions.items():
                strikes = []
                premiums = []
                types = []
                actions = []

                # Aggregate data for all legs in this position
                # Note: Assuming pos has a 'legs' attribute or we use strikes directly
                # In our case, Position stores long_strike and short_strike.

                # Short Leg
                s_prices = market_data_store.get_prices(
                    pos.short_strike, pos.option_type
                )
                if s_prices:
                    strikes.append(float(pos.short_strike))
                    premiums.append(s_prices.get("mid", 0.0))
                    types.append(pos.option_type)
                    actions.append(-1)  # Short

                # Long Leg
                l_prices = market_data_store.get_prices(
                    pos.long_strike, pos.option_type
                )
                if l_prices:
                    strikes.append(float(pos.long_strike))
                    premiums.append(l_prices.get("mid", 0.0))
                    types.append(pos.option_type)
                    actions.append(1)  # Long

                if len(strikes) < 2:
                    continue

                # 3. Compute Greeks for the batch
                results = greeks_engine.compute_batch(
                    spot=spot,
                    strikes=np.array(strikes),
                    premiums=np.array(premiums),
                    option_types=np.array(types),
                    time_to_expiry=time_to_expiry,
                )

                # 4. Aggregate Net Greeks (Action-weighted)
                # action: -1 for Sell, +1 for Buy
                net_delta = 0.0
                net_theta = 0.0

                for i in range(len(actions)):
                    net_delta += results["delta"][i] * actions[i] * pos.lots
                    net_theta += results["theta"][i] * actions[i] * pos.lots

                # 5. Update Portfolio State
                portfolio_state.update_greeks(
                    position_id=pid,
                    net_delta=net_delta,
                    net_gamma=0.0,  # Optional: can add later
                    net_theta=net_theta,
                    net_vega=0.0,
                )

        except Exception as e:
            logger.debug(f"[GREEKS MONITOR] Update cycle failed: {e}")

    async def _job_market_open(self) -> None:
        logger.info("[SCHEDULER] ⏰ 09:15 — Market open sequence starting...")
        from data.websocket_manager import ws_manager
        from positions.lifecycle_manager import lifecycle_manager
        from risk.portfolio_state import portfolio_state
        from config.settings import settings
        from analytics.worker_process import worker_process
        from data.runtime_config import runtime_config

        self.ws_manager = ws_manager
        self.lifecycle_manager = lifecycle_manager

        if self.auth_engine:
            ws_manager.auth_engine = self.auth_engine

        # --- NEW: Compound Capital Initialization ---
        try:
            from persistence.database import fetchrow

            last_session = await fetchrow(
                "SELECT starting_capital, ending_pnl FROM sessions ORDER BY date DESC LIMIT 1"
            )
            if last_session:
                compounded_capital = float(last_session["starting_capital"]) + float(
                    last_session["ending_pnl"]
                )
                logger.info(
                    f"[SCHEDULER] Compounded capital fetched: ₹{compounded_capital:,.2f}"
                )
            else:
                compounded_capital = settings.total_capital
                logger.info(
                    f"[SCHEDULER] No previous session. Using default capital: ₹{compounded_capital:,.2f}"
                )
        except Exception as e:
            logger.error(f"[SCHEDULER] Failed to fetch compounded capital: {e}")
            compounded_capital = settings.total_capital

        portfolio_state.initialize_session(
            session_date=date.today(),
            starting_capital=compounded_capital,
        )

        # --- NEW: DB Session Initialization ---
        try:
            from persistence.database import fetchrow, execute

            today = date.today()

            # Check if session already exists (e.g. on restart)
            row = await fetchrow("SELECT id FROM sessions WHERE date = $1", today)
            if not row:
                await execute(
                    "INSERT INTO sessions (date, starting_capital) VALUES ($1, $2)",
                    today,
                    compounded_capital,
                )
                row = await fetchrow("SELECT id FROM sessions WHERE date = $1", today)

            if row:
                portfolio_state._session_id = row["id"]
                logger.info(
                    f"[SCHEDULER] DB Session ID resolved: {portfolio_state._session_id}"
                )
        except Exception as e:
            logger.error(f"[SCHEDULER] Failed to initialize DB session: {e}")
        # --------------------------------------

        try:
            worker_process.start()
            self.worker_process = worker_process
            logger.info("[SCHEDULER] Analytics worker process started.")
        except Exception as exc:
            logger.error("[SCHEDULER] Worker process failed to start: {}", exc)
            await send_alert(f"CRITICAL: Analytics worker process failed — {exc}")

        # --- Reset VWAP so pre-market ticks don't contaminate the session ---
        from analytics.vwap_engine import vwap_engine

        vwap_engine.reset()
        logger.info(
            "[SCHEDULER] VWAP engine reset and firing flags cleared for new session."
        )
        logger.info(
            "[SCHEDULER] VWAP engine reset and firing flags cleared for new session."
        )
        # -----------------------------------------------------------------

        # --- THE FIX: Inject the REST Snapshot before evaluating signals ---
        nifty = runtime_config.instruments.get("NIFTY")
        if nifty:
            await self._seed_market_data_snapshot(nifty)
            await self._job_refresh_margin()
        # -----------------------------------------------------------------

        await lifecycle_manager.start()

        self._market_open = True
        self._signal_task = asyncio.create_task(
            self._signal_evaluation_loop(), name="signal_evaluation"
        )
        logger.info("[SCHEDULER] Signal evaluation loop started.")

        # --- THE HOT-BOOT FIX: If we start mid-day, check if PCR should be active now ---
        if datetime.now().time() >= PCR_ACTIVATION_TIME:
            logger.info(
                "[SCHEDULER] Catching up: Current time is past activation window. Activating PCR Engine now."
            )
            await self._job_activate_pcr()
            await self._job_refresh_margin()

        # --- THE SCIENTIFIC ATR SEED: Runs every boot to ensure sizer is ready ---
        if nifty:
            await self._seed_atr_engine(nifty)
        # -----------------------------------------------------------------------------

        asyncio.create_task(ws_manager.start(), name="websocket_manager")
        logger.info("[SCHEDULER] ✅ Market open sequence complete.")

    async def _job_refresh_margin(self) -> None:
        """Every 15 min — fetches live Iron Condor margin (all 4 legs) from Dhan API."""
        from execution.margin_cache import margin_cache
        from data.runtime_config import runtime_config
        from config.constants import SPREAD_WIDTH

        try:
            nifty = runtime_config.instruments.get("NIFTY")
            if not nifty:
                return

            atm_strike = self._compute_atm(nifty)
            if atm_strike is None:
                return

            interval = nifty.strike_interval  # 50
            width_pts = SPREAD_WIDTH * interval  # 300

            # Iron Condor legs (Calculate all 4 for true portfolio margin)
            short_ce = atm_strike + interval
            long_ce = short_ce + width_pts
            short_pe = atm_strike - interval
            long_pe = short_pe - width_pts

            tokens = nifty.instrument_tokens
            t_short_ce = tokens.get(short_ce, {}).get("CE")
            t_long_ce = tokens.get(long_ce, {}).get("CE")
            t_short_pe = tokens.get(short_pe, {}).get("PE")
            t_long_pe = tokens.get(long_pe, {}).get("PE")

            if not all([t_short_ce, t_long_ce, t_short_pe, t_long_pe]):
                logger.warning(
                    "[SCHEDULER] Margin refresh: Iron Condor tokens missing. "
                    "Using cached margin."
                )
                return

            margin_cache.refresh(
                lot_size=nifty.lot_size,
                legs=[
                    {"token": str(t_short_ce), "action": "SELL"},
                    {"token": str(t_long_ce), "action": "BUY"},
                    {"token": str(t_short_pe), "action": "SELL"},
                    {"token": str(t_long_pe), "action": "BUY"},
                ],
            )

        except Exception as exc:
            logger.warning(f"[SCHEDULER] Periodic margin cache refresh failed: {exc}")

    async def _job_activate_pcr(self) -> None:
        from analytics.pcr_engine import pcr_engine

        self._pcr_active = True
        pcr_engine.activate()
        logger.info(
            f"[SCHEDULER] ⏰ {PCR_ACTIVATION_TIME.strftime('%H:%M')} — PCR engine activated. Gate 2 now evaluable."
        )

    async def _job_ivr_update(self) -> None:
        if not self._market_open:
            return
        from analytics.ivr_engine import ivr_engine
        from data.market_data_store import market_data_store
        from data.runtime_config import runtime_config

        try:
            nifty = runtime_config.instruments.get("NIFTY")
            if not nifty:
                return
            atm_strike = self._compute_atm(nifty)

            # Retrieve Futures LTP for periodic display
            fut_prices = market_data_store.get_prices(nifty.futures_token, "FUT")
            fut_ltp = fut_prices.get("mid", 0.0)

            # Use VWAP engine's last price as live futures spot (always up-to-date)
            from analytics.vwap_engine import vwap_engine

            fut_ltp = vwap_engine.last_price if vwap_engine.last_price > 0 else fut_ltp

            # Fetch true India VIX from market data store
            atm_iv = market_data_store.get_index("INDIA_VIX")

            # --- THE FIX: Instrumented IV Update ---
            if atm_iv > 0:
                ivr = ivr_engine.update(atm_iv, datetime.now())
                logger.info(
                    "[SCHEDULER] Spot: ₹{:.2f} | ATM: {} | IVR: {:.2f} | Regime: {}",
                    fut_ltp,
                    atm_strike,
                    ivr,
                    ivr_engine.regime,
                )
            else:
                logger.warning(
                    f"[SCHEDULER] India VIX tick missing. Data store empty or not connected."
                )
            # ---------------------------------------

        except Exception as exc:
            logger.warning("[SCHEDULER] IVR update failed: {}", exc)

    async def _job_oi_wall_update(self) -> None:
        if not self._market_open:
            return
        from analytics.oi_wall_scanner import oi_wall_scanner
        from data.market_data_store import market_data_store
        from data.runtime_config import runtime_config

        try:
            nifty = runtime_config.instruments.get("NIFTY")
            if not nifty:
                return
            atm_strike = self._compute_atm(nifty)
            # Build current total_oi snapshot from market_data_store
            current_oi = {}
            for strike in market_data_store.tracked_strikes:
                entry = {}
                for opt in ("CE", "PE"):
                    prices = market_data_store.get_prices(strike, opt)
                    if prices:
                        entry[opt] = {"total_oi": prices.get("oi", 0)}
                if entry:
                    current_oi[strike] = entry

            if not current_oi:
                return

            # Feed the wall scanner (expects nested total_oi dicts)
            oi_wall_scanner.update(atm_strike, current_oi)

            from analytics.pcr_engine import pcr_engine

            pcr_engine.update_spot(atm_strike)

            # Calculate change-in-OI (delta) since last run
            if self._prev_oi_data:
                oi_delta = {}
                for strike, types in current_oi.items():
                    delta_types = {}
                    for opt, total_dict in types.items():
                        curr_val = total_dict.get("total_oi", 0)
                        prev_val = (
                            self._prev_oi_data.get(strike, {})
                            .get(opt, {})
                            .get("total_oi", 0)
                        )
                        # --- IMPROVEMENT: Include unwinding (Negative Changes) for board market view ---
                        delta_types[opt] = curr_val - prev_val
                    oi_delta[strike] = delta_types

                signal = pcr_engine.update(oi_delta, atm_strike)
                logger.info(
                    f"[PCR ENGINE] Δ-OI Snapshot | PCR: {pcr_engine.pcr:.4f} | Signal: {signal}"
                )

            # Store current as previous for next run
            self._prev_oi_data = current_oi

        except Exception as exc:
            logger.warning("[SCHEDULER] OI wall/PCR update failed: {}", exc)

    async def _job_update_macro_sma(self) -> None:
        from analytics.pcr_engine import pcr_engine

        try:
            await pcr_engine.update_macro_sma()
            logger.info("[SCHEDULER] 20-day macro SMA updated.")
        except Exception as exc:
            logger.warning("[SCHEDULER] Macro SMA update failed: {}", exc)

    async def _job_eod_reporting_and_kill(self) -> None:
        """
        Runs at 15:10 PM.
        - Liquidates and Reports ONLY if it is an Expiry Day.
        """
        logger.info("[SCHEDULER] ⏰ 03:10 — Expiry Check initiated.")

        # 1. Determine Expiry
        is_expiry = False
        try:
            from data.runtime_config import runtime_config

            nifty = runtime_config.instruments.get("NIFTY")
            is_expiry = nifty and nifty.next_expiry == date.today()
        except Exception as e:
            logger.warning(f"[SCHEDULER] Could not determine expiry: {e}")

        # 2. Hard Kill and Report only on Expiry
        if is_expiry:
            logger.info(
                "[SCHEDULER] Expiry day detected: Triggering Hard Kill and EOD Report."
            )
            if self.lifecycle_manager:
                await self.lifecycle_manager._hard_kill()

            if not self._report_sent:
                await self._generate_and_dispatch_report()
        else:
            logger.info("[SCHEDULER] Non-expiry day: Reporting deferred to 15:30.")

    async def _job_final_shutdown(self) -> None:
        """
        Runs at 15:30 PM.
        - Reports if not already done (non-expiry).
        - Final system shutdown.
        """
        if self._session_ended:
            return

        logger.info("[SCHEDULER] ⏰ 03:30 — Final system shutdown sequence.")

        if not self._report_sent:
            logger.info(
                "[SCHEDULER] Report safety net triggered: Generating final session report."
            )
            await self._generate_and_dispatch_report()

        self._session_ended = True
        self._market_open = False

        from analytics.ivr_engine import ivr_engine
        from risk.portfolio_state import portfolio_state
        from persistence.write_buffer import write_buffer

        # 2. Stop Signal Task and Managers
        if self._signal_task and not self._signal_task.done():
            self._signal_task.cancel()

        if self.lifecycle_manager:
            await self.lifecycle_manager.stop()

        if self.ws_manager:
            await self.ws_manager.stop()

        if self.worker_process:
            try:
                self.worker_process.stop()
            except Exception as exc:
                logger.warning(f"Worker process stop error: {exc}")

        # 3. Save EOD IV
        try:
            from data.market_data_store import market_data_store

            atm_iv_close = market_data_store.get_index("INDIA_VIX")
            if atm_iv_close > 0:
                await ivr_engine.end_of_day(atm_iv_close)
        except Exception as exc:
            logger.warning(f"IVR EOD record failed: {exc}")

        # 4. Final Data Save
        self._save_session_flags(portfolio_state.total_capital)
        await write_buffer.stop()

        logger.info("[SCHEDULER] ✅ Final shutdown complete.")
        if self.shutdown_event is not None:
            self.shutdown_event.set()

    async def _generate_and_dispatch_report(self) -> None:
        """Consolidated logic to generate and send the Telegram report."""
        from risk.portfolio_state import portfolio_state
        from monitoring.reporter import (
            dispatch_eod_report,
            SessionSummary,
            ClosedTrade,
            CarryPosition,
        )
        from persistence.write_buffer import write_buffer
        from analytics.ivr_engine import ivr_engine
        from decouple import config

        try:
            # 1. Calculate Session Stats for DB
            wins = sum(
                1
                for t in portfolio_state._closed_positions.values()
                if t.realized_pnl > 0
            )
            losses = sum(
                1
                for t in portfolio_state._closed_positions.values()
                if t.realized_pnl <= 0
            )
            total = wins + losses
            win_rate = (wins / total * 100) if total > 0 else 0.0

            # 2. Update Session Record in DB
            await write_buffer.put(
                "sessions",
                (
                    date.today(),
                    portfolio_state._starting_capital,
                    portfolio_state.realized_pnl,
                    portfolio_state._new_trades_count,
                    wins,
                    losses,
                    win_rate,
                    ivr_engine.regime,
                    portfolio_state.circuit_breaker_tripped,
                ),
            )

            # 2. Map Closed Trades
            closed_trades_list = []
            for pid, pos in portfolio_state._closed_positions.items():
                strikes_desc = f"{pos.short_strike}{pos.option_type}/{pos.long_strike}{pos.option_type}"
                closed_trades_list.append(
                    ClosedTrade(
                        entry_time=pos.entry_timestamp,
                        exit_time=pos.exit_timestamp or datetime.now(),
                        direction="BULL" if pos.option_type == "PE" else "BEAR",
                        strategy=pos.spread_type,
                        strikes=strikes_desc,
                        lots=pos.lots,
                        gross_pnl=pos.gross_pnl,
                        entry_costs=pos.entry_costs,
                        exit_costs=pos.exit_costs,
                        exit_reason=pos.exit_reason or "UNKNOWN",
                        entry_premium=pos.net_entry_premium,
                        exit_premium=pos.net_exit_premium,
                        entry_slippage=0.0,
                        exit_slippage=0.0,
                    )
                )

            # 3. Map Carry Positions
            carry_over_list = []
            total_upnl = 0.0
            for pid, pos in portfolio_state.open_positions.items():
                strikes_desc = f"{pos.short_strike}{pos.option_type}/{pos.long_strike}{pos.option_type}"
                carry_over_list.append(
                    CarryPosition(
                        strategy=pos.spread_type,
                        strikes=strikes_desc,
                        lots=pos.lots,
                        unrealized_pnl=pos.unrealized_pnl,
                    )
                )
                total_upnl += pos.unrealized_pnl

                # 4. Dispatch
                summary = SessionSummary(
                    session_date=date.today(),
                    starting_capital=portfolio_state._starting_capital,
                    live_trading=config("LIVE_TRADING", default=False, cast=bool),
                    closed_trades=closed_trades_list,
                    carry_over_positions=carry_over_list,
                    total_unrealized_pnl=total_upnl,
                    regime_log=self.regime_log,
                    circuit_breaker_tripped=portfolio_state.circuit_breaker_tripped,
                    new_trades_count=portfolio_state._new_trades_count,
                )
            dispatch_eod_report(summary)
            self._report_sent = True
            logger.info("[SCHEDULER] EOD Report successfully dispatched.")
        except Exception as exc:
            logger.error(
                f"[SCHEDULER] Failed to generate/dispatch report: {exc}", exc_info=True
            )

    async def _job_heartbeat(self) -> None:
        if self.heartbeat_counter is not None:
            from monitoring.health_check import increment_heartbeat

            increment_heartbeat(self.heartbeat_counter)

    async def _job_minute_stats(self) -> None:
        """Tracks time spent in each IVR regime for the EOD report."""
        if not self._market_open:
            return

        from analytics.ivr_engine import ivr_engine

        regime = ivr_engine.regime

        if regime == "CREDIT":
            self.regime_log.credit_minutes += 1
        elif regime == "DEBIT":
            self.regime_log.debit_minutes += 1
        else:
            self.regime_log.standby_minutes += 1

    async def _job_reconcile_positions(self) -> None:
        if not self._market_open:
            return
        if not config("LIVE_TRADING", default=False, cast=bool):
            return
        from risk.reconciliation import reconciliation_engine

        try:
            await reconciliation_engine.run_reconciliation()
        except Exception as exc:
            logger.warning("[SCHEDULER] Reconciliation audit failed: {}", exc)

    async def _job_theta_harvest(self) -> None:
        """Every 30 minutes — evaluate open credit positions for early profit taking."""
        if not self._market_open:
            return

        from analytics.theta_tracker import theta_tracker

        try:
            await theta_tracker.sweep()
        except Exception as exc:
            logger.warning("[SCHEDULER] Theta harvest sweep failed: {}", exc)

    async def _signal_evaluation_loop(self) -> None:
        """
        The Main Execution Loop — matches backtest_engine.py exactly.

        On confirmation: submit the specific 2-leg spread (CREDIT_PUT or CREDIT_CALL)
        based on the directional bias (BULLISH/BEARISH). Identical to the backtest.
        """
        logger.info(
            "[SCHEDULER] Signal evaluation loop running (Vertical Spread mode — matches backtest)."
        )

        while self._market_open and not self._session_ended:
            await asyncio.sleep(SIGNAL_EVAL_INTERVAL)

            try:
                from risk.pre_execution import pre_execution_gate
                from signals.regime_gate import regime_gate
                from signals.trigger_gate import trigger_gate
                from orders.order_manager import order_manager
                from data.runtime_config import runtime_config
                from analytics.ivr_engine import ivr_engine
                from risk.portfolio_state import portfolio_state
                from datetime import time as dt_time

                nifty = runtime_config.instruments.get("NIFTY")
                if not nifty:
                    continue

                # --- THE DATA INTERLOCK: STANDBY TRIGGER ---
                atm_strike = self._compute_atm(nifty)
                now = datetime.now()

                if atm_strike is None:
                    logger.warning(
                        "[SIGNAL] STANDBY: Live market data missing or zero. Evaluation paused."
                    )
                    continue
                # --------------------------------------------

                is_expiry_day = nifty.next_expiry == date.today()

                # Gate 1: Pre-Execution (Blackouts / Expiry / Circuit Breaker / Position Limits)
                allowed, reason = await pre_execution_gate.check(
                    is_expiry_day=is_expiry_day
                )
                if not allowed:
                    _silent = (
                        "BLACKOUT",
                        "OUTSIDE_EXECUTION_WINDOW",
                        "GAP_TRACKER",
                        "EXPIRY_DAY_CUTOFF_REACHED",
                    )
                    if not any(s in reason for s in _silent):
                        logger.warning("[SIGNAL] Pre-execution blocked: {}", reason)
                    else:
                        logger.debug("[SIGNAL] Blocked: {}", reason)
                    continue

                # Gate 2: Regime / VIX Range (matches backtest min_vix / max_vix check)
                regime_result = regime_gate.evaluate()
                if not regime_result.allowed:
                    continue

                # --- PULSE CONTROL: Fire only once per window (matches backtest) ---
                is_afternoon = now.time() >= dt_time(13, 0)

                if not is_afternoon:
                    if self._morning_fired:
                        continue
                    # Positional Rule: If already holding 1+ positions from yesterday, do not add more in the morning.
                    if portfolio_state.position_count > 0:
                        continue
                if is_afternoon:
                    if self._afternoon_fired:
                        continue
                    # Backtest: afternoon entry only allowed if position_count == 0
                    if portfolio_state.position_count > 0:
                        continue
                # -----------------------------------------------------------------

                # Gate 3: Directional Bias (DISABLED FOR IRON CONDOR)
                # The backtest ignores PCR direction and trades both sides (Delta Neutral).
                # from signals.bias_gate import bias_gate
                # bias_result = bias_gate.evaluate(regime=regime_result.regime, pcr_active=True)
                # if not bias_result.allowed:
                #     continue

                # Gate 4: VWAP Pullback Trigger (price proximity check)
                # We pass "NEUTRAL" because the Iron Condor is direction-agnostic.
                trigger_result = trigger_gate.evaluate(direction="NEUTRAL")
                if not trigger_result.confirmed:
                    continue

                # Collect live data for the final trade basket
                market_data = self._collect_market_data(nifty)
                if not market_data:
                    logger.debug("[SIGNAL] Market data snapshot empty — skipping.")
                    continue

                # --- 4-LEG IRON CONDOR: Submit BOTH sides simultaneously (matches backtest) ---
                success_put, reason_put = await order_manager.submit_basket(
                    spread_type="CREDIT_PUT",
                    atm_strike=atm_strike,
                    market_data=market_data,
                )

                success_call, reason_call = await order_manager.submit_basket(
                    spread_type="CREDIT_CALL",
                    atm_strike=atm_strike,
                    market_data=market_data,
                )

                if success_put or success_call:
                    if is_afternoon:
                        self._afternoon_fired = True
                    else:
                        self._morning_fired = True

                    self._save_session_flags(runtime_config.risk.total_capital)
                    trigger_gate.reset()

                    logger.info(
                        "[SIGNAL] IRON CONDOR submitted. ATM: {} | IVR: {} | Status: OK",
                        atm_strike,
                        ivr_engine.ivr,
                    )
                    await asyncio.sleep(5)
                else:
                    logger.debug(
                        f"[SIGNAL] Filtered: PE({reason_put}) | CE({reason_call})"
                    )

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(
                    "[SIGNAL] Unhandled error in evaluation loop: {}",
                    exc,
                    exc_info=True,
                )
                await asyncio.sleep(5)

        logger.info("[SCHEDULER] Signal evaluation loop exited.")

    def _save_session_flags(self, final_balance: float):
        """Persists the capital and session fire status to disk."""
        import json
        import os

        state_path = os.path.join("data", "capital_state.json")
        try:
            state = {
                "last_updated": datetime.now().isoformat(),
                "final_balance": final_balance,
                "morning_fired": self._morning_fired,
                "afternoon_fired": self._afternoon_fired,
            }
            with open(state_path, "w") as f:
                json.dump(state, f, indent=4)
            logger.debug("[SCHEDULER] Session flags persisted to disk.")
        except Exception as e:
            logger.error(f"[SCHEDULER] Emergency flag save failed: {e}")

    async def _fetch_spot_approximation(self) -> Optional[float]:
        """
        BOOTSTRAP SEED: Robust REST fetch to center the Scrip Master.
        Deep-parses the Dhan v2 response to find the Nifty Spot price.
        """
        import aiohttp
        from persistence.database import fetchval

        # 1. Try Database (Sanity Check: Nifty is > 15,000. Reject IV values)
        try:
            db_val = await fetchval(
                "SELECT iv_close FROM iv_history WHERE symbol = 'NIFTY' ORDER BY date DESC LIMIT 1"
            )
            if db_val and float(db_val) > 15000:
                logger.info(f"[SCHEDULER] Valid DB Seed found: {db_val}")
                return float(db_val)
        except Exception:
            pass

        # 2. Robust REST Seed (Dhan Token 13)
        headers = self.auth_engine.get_headers()
        url = "https://api.dhan.co/v2/marketfeed/ltp"
        payload = {"IDX_I": [13]}

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, headers=headers, json=payload, timeout=10
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            raw_data = data.get("data", {})

                            # Deep-parse for the price (Token 13)
                            # Dhan often keys indices directly by their token string in the data dict
                            token_data = raw_data.get("13") or raw_data.get(
                                "IDX_I", {}
                            ).get("13")
                            ltp = (
                                token_data.get("last_price", 0.0) if token_data else 0.0
                            )

                            if ltp > 15000:
                                logger.info(
                                    f"[SCHEDULER] Live REST Seed successful: ₹{ltp}"
                                )
                                return float(ltp)

                        logger.warning(
                            f"[SCHEDULER] Seed Attempt {attempt+1} failed: HTTP {resp.status}"
                        )
            except Exception as e:
                logger.error(f"[SCHEDULER] Seed Attempt {attempt+1} Error: {e}")

            await asyncio.sleep(2)

        return None

    def _compute_atm(self, nifty) -> Optional[int]:
        """
        DYNAMIC ROUNDING: Derives ATM from live Futures LTP.
        Returns None on data loss, forcing the loop into STANDBY mode.
        """
        from data.market_data_store import market_data_store

        # 1. Pull the live Futures price (Token 66691) from the memory bank
        prices = market_data_store.get_prices(nifty.futures_token, "FUT")
        ltp = prices.get("mid", 0.0)

        # 2. INTERLOCK: No live price = No trade. No hardcoded fallbacks allowed.
        if ltp <= 0:
            return None

        # 3. Mathematical Rounding: Snaps LTP to the nearest strike interval
        interval = nifty.strike_interval
        return int(round(ltp / interval) * interval)

    def _collect_market_data(self, nifty) -> dict:
        from data.market_data_store import market_data_store

        market_data = {}
        for strike, tokens in nifty.instrument_tokens.items():
            entry = {}
            for option_type in ("CE", "PE"):
                prices = market_data_store.get_prices(strike, option_type)
                if prices:
                    entry[option_type] = {
                        "bid": prices["bid"],
                        "ask": prices["ask"],
                    }
            if entry:
                market_data[strike] = entry
        return market_data

    async def _seed_atr_engine(self, nifty) -> None:
        """
        Fetches last 14+ candles of 15m Futures data to seed the ATR engine.
        Ensures position sizer is ready with SCIENTIFIC data instantly.
        """
        from analytics.atr_engine import atr_engine
        import aiohttp
        from datetime import datetime, timedelta

        logger.info("[SCHEDULER] Seeding ATR engine with historical 15m candles...")

        try:
            # Fetch last 4 days to ensure we span across weekends/holidays
            to_date = datetime.now()
            from_date = to_date - timedelta(days=4)

            headers = self.auth_engine.get_headers()
            url = "https://api.dhan.co/v2/charts/intraday"

            payload = {
                "securityId": str(nifty.futures_token),
                "exchangeSegment": "NSE_FNO",
                "instrument": "FUTIDX",
                "interval": 15,
                "fromDate": from_date.strftime("%Y-%m-%d %H:%M:%S"),
                "toDate": to_date.strftime("%Y-%m-%d %H:%M:%S"),
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, headers=headers, json=payload, timeout=15
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info(
                            f"[SCHEDULER] ATR Seed API returned {len(data.get('timestamp', []))} candles."
                        )
                        ts = data.get("timestamp", [])
                        opens = data.get("open", [])
                        highs = data.get("high", [])
                        lows = data.get("low", [])
                        closes = data.get("close", [])

                        candles = []
                        for i in range(len(ts)):
                            candles.append(
                                {
                                    "open": float(opens[i]),
                                    "high": float(highs[i]),
                                    "low": float(lows[i]),
                                    "close": float(closes[i]),
                                }
                            )

                        if len(candles) >= 2:
                            # Feed to engine
                            atr_engine.seed_candles(candles)
                        else:
                            logger.warning(
                                "[SCHEDULER] Not enough historical candles for ATR seeding. Falls back to default."
                            )
                    else:
                        resp_text = await resp.text()
                        logger.warning(
                            f"[SCHEDULER] ATR seed API failed: {resp.status}. Response: {resp_text[:100]}. Falls back to default."
                        )

        except Exception as e:
            logger.error(f"[SCHEDULER] ATR seeding process error: {e}", exc_info=True)


market_scheduler = MarketScheduler()
