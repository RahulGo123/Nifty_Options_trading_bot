import asyncio
import time
from loguru import logger
from config.constants import OI_SNAPSHOT_INTERVAL
from data.runtime_config import runtime_config


class OITracker:
    """
    Maintains a running snapshot of Open Interest for all
    tracked strikes and computes Change in OI every 60 seconds.

    Why Change in OI and not Total OI for PCR?
    Total OI accumulates over weeks and reflects historical
    positioning. Change in OI shows what is being built TODAY
    — fresh Put writing signals institutional bullish positioning,
    fresh Call writing signals bearish positioning.
    Using Total OI for PCR gives stale signals. Using Change in OI
    gives real-time sentiment.

    Data source:
    OI values come from the options_queue ticks. Each tick
    from the WebSocket contains the current OI for that
    instrument. The tracker reads from the latest tick data
    held in the options data store (populated by the tick
    dispatcher).

    TODO: The exact OI field name in MO WebSocket ticks
    must be verified against API documentation.
    Currently assumed as 'oi' — same as tick_dispatcher.py.
    """

    def __init__(self):
        # Current OI snapshot: {strike: {'CE': oi, 'PE': oi}}
        self._current_oi: dict = {}

        # Previous OI snapshot for delta calculation
        self._previous_oi: dict = {}

        # Change in OI since last snapshot
        self._oi_delta: dict = {}

        self._last_snapshot_time: float = 0.0
        self._snapshot_count: int = 0
        self._is_running: bool = False

    def update_from_tick(self, strike: int, option_type: str, oi: int) -> None:
        """
        Called by the options data consumer on every tick
        that contains a non-zero OI value.

        Maintains the most recent OI per strike per option type.
        This is called frequently — must be fast.
        """
        if strike not in self._current_oi:
            self._current_oi[strike] = {"CE": 0, "PE": 0}

        self._current_oi[strike][option_type] = oi

    def take_snapshot(self) -> dict:
        """
        Called every OI_SNAPSHOT_INTERVAL seconds.
        Computes Change in OI = current OI - previous snapshot OI.
        Stores the delta for PCR consumption.

        Returns the current delta dict.
        """
        if not self._current_oi:
            return {}

        # Compute delta for each strike and option type
        new_delta = {}
        for strike, pair in self._current_oi.items():
            prev = self._previous_oi.get(strike, {"CE": 0, "PE": 0})
            new_delta[strike] = {
                "CE": pair["CE"] - prev.get("CE", 0),
                "PE": pair["PE"] - prev.get("PE", 0),
            }

        # Store current as previous for next snapshot
        self._previous_oi = {
            strike: dict(pair) for strike, pair in self._current_oi.items()
        }
        self._oi_delta = new_delta
        self._last_snapshot_time = time.monotonic()
        self._snapshot_count += 1

        logger.debug(
            f"[OI TRACKER] Snapshot {self._snapshot_count} taken. "
            f"Tracking {len(new_delta)} strikes."
        )
        return new_delta

    async def run(self) -> None:
        """
        Background coroutine that takes OI snapshots
        every OI_SNAPSHOT_INTERVAL seconds.
        Started at 09:15 AM alongside the WebSocket.
        """
        self._is_running = True
        logger.info(
            f"[OI TRACKER] Started. " f"Snapshot interval: {OI_SNAPSHOT_INTERVAL}s"
        )

        while self._is_running:
            await asyncio.sleep(OI_SNAPSHOT_INTERVAL)
            self.take_snapshot()

    def stop(self) -> None:
        """Called during 03:10 PM hard kill."""
        self._is_running = False
        logger.info("[OI TRACKER] Stopped.")

    @property
    def delta(self) -> dict:
        """
        Most recent Change in OI snapshot.
        Structure: {strike: {'CE': delta_oi, 'PE': delta_oi}}
        """
        return self._oi_delta

    @property
    def current_oi(self) -> dict:
        """
        Most recent Total OI per strike.
        Used by OI Wall Scanner — not by PCR engine.
        Structure: {strike: {'CE': total_oi, 'PE': total_oi}}
        """
        return self._current_oi

    @property
    def is_ready(self) -> bool:
        """
        True once at least one snapshot has been taken.
        PCR engine waits for this before activating.
        """
        return self._snapshot_count >= 1


# Single instance
oi_tracker = OITracker()
