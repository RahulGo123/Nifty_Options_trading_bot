import asyncio
from datetime import datetime
from loguru import logger
from risk.portfolio_state import portfolio_state
from persistence.write_buffer import write_buffer
from data.market_data_store import market_data_store
from data.runtime_config import runtime_config


class GreeksMonitor:
    """
    Snapshots Greeks for all open positions every 5 minutes.
    Queues snapshots to the write buffer for database
    persistence.

    Why every 5 minutes and not every tick?
    Greeks change slowly relative to tick frequency.
    A 5-minute snapshot is sufficient for:
    - Position monitoring and reporting
    - Theta decay tracking
    - Portfolio-level risk assessment

    The Greeks values come from the analytics worker process
    result queue. The monitor reads the latest available
    result and stores it against each open position.
    """

    SNAPSHOT_INTERVAL = 300  # 5 minutes in seconds

    def __init__(self):
        self._running: bool = False
        self._snapshot_count: int = 0

    async def run(self, result_queue) -> None:
        """
        Background coroutine that snapshots Greeks every
        5 minutes for all open positions.

        result_queue: multiprocessing.Queue from the
                      Greeks worker process.
        """
        self._running = True
        logger.info("[GREEKS MONITOR] Started.")

        while self._running:
            await asyncio.sleep(self.SNAPSHOT_INTERVAL)

            if not portfolio_state.open_positions:
                continue

            # Get the latest Greeks result from the worker
            greeks_result = self._get_latest_greeks(result_queue)

            if not greeks_result:
                logger.warning(
                    "[GREEKS MONITOR] No Greeks result available. "
                    "Worker may not have processed a batch yet."
                )
                continue

            await self._snapshot_all_positions(greeks_result)

    def _get_latest_greeks(self, result_queue) -> dict:
        """
        Drains the result queue to get the most recent
        Greeks computation result.
        Discards all but the latest result.
        """
        latest = None
        while not result_queue.empty():
            try:
                latest = result_queue.get_nowait()
            except Exception:
                break
        return latest

    async def _snapshot_all_positions(self, greeks_result: dict) -> None:
        """
        Takes a Greeks snapshot for every open position
        and queues it to the write buffer.
        """
        now = datetime.now()
        strikes_arr = greeks_result.get("strikes", [])
        iv_arr = greeks_result.get("iv", [])
        delta_arr = greeks_result.get("delta", [])
        gamma_arr = greeks_result.get("gamma", [])
        theta_arr = greeks_result.get("theta", [])
        vega_arr = greeks_result.get("vega", [])

        # Build a lookup from strike to Greeks index
        strike_to_idx = {int(strikes_arr[i]): i for i in range(len(strikes_arr))}

        for pos_id, position in portfolio_state.open_positions.items():
            # Get Greeks for long and short legs
            long_idx = strike_to_idx.get(position.long_strike)
            short_idx = strike_to_idx.get(position.short_strike)

            if long_idx is None or short_idx is None:
                logger.debug(
                    f"[GREEKS MONITOR] Greeks not available for "
                    f"position {pos_id} strikes "
                    f"{position.long_strike}/{position.short_strike}"
                )
                continue

            # Net Greeks for the spread
            # For a long+short spread, Greeks net out
            net_delta = float(delta_arr[long_idx]) - float(delta_arr[short_idx])
            net_gamma = float(gamma_arr[long_idx]) - float(gamma_arr[short_idx])
            net_theta = float(theta_arr[long_idx]) - float(theta_arr[short_idx])
            net_vega = float(vega_arr[long_idx]) - float(vega_arr[short_idx])
            iv_atm = float(iv_arr[long_idx])

            # Update in-memory position
            portfolio_state.update_greeks(
                pos_id, net_delta, net_gamma, net_theta, net_vega
            )

            # Queue snapshot for database
            # --- MODIFIED: Deferred Persistence ---
            # Ghost trade IDs are not known until close.
            # snapshots are buffered in memory and flushed at close.
            position.snapshots.append((
                datetime.now(),
                position.unrealized_pnl,
                net_delta,
                net_theta,
                net_gamma,
                net_vega,
                iv_atm,
            ))
            # ---------------------------------------

        self._snapshot_count += 1
        logger.debug(
            f"[GREEKS MONITOR] Snapshot {self._snapshot_count} "
            f"taken for {len(portfolio_state.open_positions)} "
            f"positions."
        )

    def stop(self) -> None:
        """Called during 03:10 PM hard kill."""
        self._running = False
        logger.info("[GREEKS MONITOR] Stopped.")


# Single instance
greeks_monitor = GreeksMonitor()
