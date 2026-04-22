import asyncio
from datetime import datetime, date
from loguru import logger
from config.constants import (
    THETA_STAGNATION_MINUTES,
    THETA_MINIMUM_GAIN_PCT,
    HARD_KILL_TIME,
    NEAR_EXPIRY_EXIT_TIME,
)
from risk.portfolio_state import portfolio_state
from orders.order_manager import order_manager
from data.market_data_store import market_data_store
from data.runtime_config import runtime_config
from monitoring.alerting import send_alert


class LifecycleManager:
    """
    Monitors all open positions and manages their exit.

    Exit triggers in priority order:
    1. Circuit breaker tripped — liquidate everything immediately
    2. Stop-loss hit — unrealized P&L crossed the stop threshold
    3. Theta stagnation — no favourable movement in 45 minutes
    4. Near-expiry forced exit — 01:30 PM on expiry day
    5. Hard kill — 03:10 PM every session

    MTM evaluation:
    Uses mid price (Bid+Ask)/2 for mark-to-market calculations.
    This gives the fair value of the position, not the
    worst-case liquidation value. Using bid/ask for MTM
    would systematically understate position value and
    trigger stops prematurely.

    Actual exit fills use Bid/Ask as handled by ghost_ledger.
    """

    STOP_CHECK_INTERVAL = 5  # seconds
    THETA_CHECK_INTERVAL = 60  # seconds
    TIME_CHECK_INTERVAL = 30  # seconds

    def __init__(self):
        self._running: bool = False
        self._session_ended: bool = False
        self._stop_task: asyncio.Task = None
        self._theta_task: asyncio.Task = None
        self._time_task: asyncio.Task = None

    async def start(self) -> None:
        """
        Starts all monitoring coroutines.
        Called at 09:15 AM alongside the WebSocket.
        """
        self._running = True
        self._session_ended = False

        self._stop_task = asyncio.create_task(
            self._stop_loss_loop(), name="lifecycle_stop_loss"
        )
        self._theta_task = asyncio.create_task(
            self._theta_stagnation_loop(), name="lifecycle_theta"
        )
        self._time_task = asyncio.create_task(
            self._time_check_loop(), name="lifecycle_time"
        )
        logger.info("[LIFECYCLE] Position lifecycle manager started.")

    async def stop(self) -> None:
        """
        Stops all monitoring coroutines cleanly.
        Called after the hard kill sequence completes.
        Does not cancel tasks from within themselves —
        sets the _running flag and lets loops exit naturally.
        """
        self._running = False

        # Give loops time to exit naturally on next iteration
        await asyncio.sleep(0.1)

        for task in [self._stop_task, self._theta_task, self._time_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        logger.info("[LIFECYCLE] Position lifecycle manager stopped.")

    # ------------------------------------------------------------------
    # Loop 1: Stop-Loss Monitor (every 5 seconds)
    # ------------------------------------------------------------------

    async def _stop_loss_loop(self) -> None:
        """
        Evaluates stop-loss conditions every 5 seconds.
        Uses mid-price for MTM — not bid/ask.
        """
        while self._running:
            await asyncio.sleep(self.STOP_CHECK_INTERVAL)

            if self._session_ended:
                return

            # Circuit breaker takes priority
            if portfolio_state.circuit_breaker_tripped:
                await self._liquidate_all("CIRCUIT_BREAKER")
                return

            for pos_id in list(portfolio_state.open_positions.keys()):
                position = portfolio_state.open_positions.get(pos_id)
                if not position:
                    continue

                # Update MTM using mid price
                self._update_mtm_mid(position)

                # Check stop loss
                if position.unrealized_pnl <= position.stop_loss_value:
                    logger.warning(
                        f"[LIFECYCLE] Stop-loss triggered: {pos_id} | "
                        f"P&L: ₹{position.unrealized_pnl:,.2f} | "
                        f"Stop: ₹{position.stop_loss_value:,.2f}"
                    )
                    await self._exit_position(pos_id, "STOP_LOSS")

    # ------------------------------------------------------------------
    # Loop 2: Theta / Stagnation Monitor (every 60 seconds)
    # ------------------------------------------------------------------

    async def _theta_stagnation_loop(self) -> None:
        """
        Theta harvest and stagnation exits are handled by theta_tracker.py
        (scheduled via market_scheduler every 30 minutes), matching the
        backtest's _check_exits which runs on every bar.
        This loop is kept for architecture compatibility but does nothing.
        """
        while self._running:
            await asyncio.sleep(300)
            if self._session_ended:
                return

    # ------------------------------------------------------------------
    # Loop 3: Time-Based Checks (every 30 seconds)
    # ------------------------------------------------------------------

    async def _time_check_loop(self) -> None:
        """
        Handles time-based exits.
        Returns after triggering hard kill or expiry exit
        to prevent repeated liquidation attempts.
        """
        near_expiry_done = False

        while self._running:
            await asyncio.sleep(self.TIME_CHECK_INTERVAL)

            if self._session_ended:
                return

            now = datetime.now()

            # Near-expiry exit — 01:30 PM on expiry day only
            if not near_expiry_done:
                nifty = runtime_config.instruments.get("NIFTY")
                if nifty:
                    is_expiry_day = nifty.next_expiry == now.date()
                    if is_expiry_day and now.time() >= NEAR_EXPIRY_EXIT_TIME:
                        if portfolio_state.open_positions:
                            logger.info(
                                "[LIFECYCLE] Expiry day forced " "exit at 01:30 PM."
                            )
                            await self._liquidate_all("NEAR_EXPIRY_FORCED_EXIT")
                        near_expiry_done = True

    # ------------------------------------------------------------------
    # Exit Helpers
    # ------------------------------------------------------------------

    async def _exit_position(
        self,
        position_id: str,
        exit_reason: str,
    ) -> bool:
        """
        Exits a single position using the order manager.
        Returns True if exit succeeded, False otherwise.
        """
        position = portfolio_state.open_positions.get(position_id)
        if not position:
            return True  # Already closed

        market_data = market_data_store.get_spread_market_data(
            long_strike=position.long_strike,
            short_strike=position.short_strike,
            option_type=position.option_type,
        )

        if market_data is None:
            logger.warning(
                f"[LIFECYCLE] No live market data for "
                f"{position_id} exit. Using fallback prices."
            )
            market_data = self._build_fallback_market_data(position)

        success = await order_manager.close_position(
            position_id=position_id,
            market_data=market_data,
            exit_reason=exit_reason,
        )

        if not success:
            logger.error(
                f"[LIFECYCLE] Failed to exit {position_id}. "
                f"Will retry on next cycle."
            )
            await send_alert(
                f"HIGH: Failed to exit position {position_id} "
                f"({exit_reason}). Will retry."
            )

        return success

    async def _liquidate_all(self, exit_reason: str) -> None:
        """
        Exits all open positions.
        Logs how many were successfully closed.
        """
        open_ids = list(portfolio_state.open_positions.keys())

        if not open_ids:
            logger.info(
                f"[LIFECYCLE] Liquidate all called but no "
                f"positions open. Reason: {exit_reason}"
            )
            return

        logger.info(
            f"[LIFECYCLE] Liquidating {len(open_ids)} positions. "
            f"Reason: {exit_reason}"
        )

        closed = 0
        failed = 0
        for pos_id in open_ids:
            success = await self._exit_position(pos_id, exit_reason)
            if success:
                closed += 1
            else:
                failed += 1

        logger.info(
            f"[LIFECYCLE] Liquidation complete. "
            f"Closed: {closed} | Failed: {failed} | "
            f"Still open: {portfolio_state.position_count}"
        )

        if failed > 0:
            await send_alert(
                f"CRITICAL: {failed} positions failed to close "
                f"during {exit_reason}. "
                f"Manual intervention required."
            )

    async def _hard_kill(self) -> None:
        """
        03:10 PM hard kill.
        Sets session_ended flag BEFORE liquidating so
        other loops stop immediately.
        Does NOT call self.stop() — the main orchestrator
        in main.py handles the full shutdown sequence.
        """
        self._session_ended = True
        await self._liquidate_all("HARD_KILL_03:10PM")

        logger.info(
            "[LIFECYCLE] Hard kill complete. "
            f"Final session P&L: "
            f"₹{portfolio_state.realized_pnl:,.2f}"
        )
        await send_alert(
            f"03:10 PM hard kill complete. "
            f"All positions closed. "
            f"Final session P&L: "
            f"₹{portfolio_state.realized_pnl:,.2f}"
        )

    def _update_mtm_mid(self, position) -> None:
        """
        Updates mark-to-market using MID price (Bid+Ask)/2.

        Mid price is the fair value of the position.
        Using bid for long and ask for short gives the
        worst-case liquidation value which would trigger
        stop-losses prematurely.

        Actual exit fills use Bid/Ask — handled by ghost_ledger.
        """
        long_data = market_data_store.get_prices(
            position.long_strike, position.option_type
        )
        short_data = market_data_store.get_prices(
            position.short_strike, position.option_type
        )

        if long_data and short_data:
            long_mid = (long_data["bid"] + long_data["ask"]) / 2
            short_mid = (short_data["bid"] + short_data["ask"]) / 2

            portfolio_state.update_mark_to_market(
                position.position_id,
                current_premium_long=long_mid,
                current_premium_short=short_mid,
            )

    def _build_fallback_market_data(self, position) -> dict:
        """
        Builds synthetic market data from last known MTM
        prices when live data is unavailable.
        Uses 0.5% spread around last known mid price.
        """
        long_mid = position.current_premium_long
        short_mid = position.current_premium_short
        spread_pct = 0.005

        return {
            position.long_strike: {
                position.option_type: {
                    "bid": round(long_mid * (1 - spread_pct), 2),
                    "ask": round(long_mid * (1 + spread_pct), 2),
                }
            },
            position.short_strike: {
                position.option_type: {
                    "bid": round(short_mid * (1 - spread_pct), 2),
                    "ask": round(short_mid * (1 + spread_pct), 2),
                }
            },
        }


# Single instance
lifecycle_manager = LifecycleManager()
