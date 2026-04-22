from collections import deque
from datetime import date
from loguru import logger
from persistence.write_buffer import write_buffer

class IVREngine:
    """
    Volatility Engine.
    Tracks raw ATM Implied Volatility (IV) and computes IV Rank (IVR).
    Maintains a 252-session rolling window for backtest parity.
    """

    def __init__(self):
        self._history: deque[float] = deque(maxlen=252)
        self._current_iv: float = 0.0
        self._session_iv_10am: float = None
        self._10am_captured: bool = False

    def bootstrap(self, historical_records: list) -> bool:
        """
        Loads historical closing IV values from PostgreSQL into memory.
        Ensures the Rank calculation is accurate from the first tick.
        """
        if not historical_records:
            logger.warning("[VOLATILITY ENGINE] No historical IV data provided for bootstrap.")
            return False

        # Records expected as list of floats (closing IVs)
        for val in historical_records:
            if val and val > 0:
                self._history.append(float(val))

        logger.info(
            f"[VOLATILITY ENGINE] Bootstrapped with {len(self._history)} historical sessions. "
            f"Ready for Rank calculation."
        )
        return True

    def update(self, atm_iv: float, timestamp) -> float:
        """
        Updates the current raw IV from the Greeks engine.
        Also captures the 10:00 AM snapshot for daily records.
        """
        self._current_iv = atm_iv

        # Capture 10:00 AM IV for daily record
        if (
            not self._10am_captured
            and hasattr(timestamp, "hour")
            and timestamp.hour == 10
            and timestamp.minute < 5
        ):
            self._session_iv_10am = atm_iv
            self._10am_captured = True
            logger.info(f"[VOLATILITY ENGINE] 10:00 AM IV captured: {atm_iv:.4f}")

        return self.ivr

    async def end_of_day(self, closing_iv: float) -> None:
        """
        Queues the daily IV record for persistence.
        """
        today = date.today()
        # Compute IVR session close (add current close to history for final calc)
        self._history.append(closing_iv)
        final_ivr = self.ivr

        await write_buffer.put(
            "iv_history",
            (
                today,
                "NIFTY",
                self._session_iv_10am,
                closing_iv,
                final_ivr,
            ),
        )

        logger.info(
            f"[VOLATILITY ENGINE] EOD record queued. "
            f"Date: {today} | IV close: {closing_iv:.4f} | IVR: {final_ivr:.2f}"
        )

    @property
    def current_iv(self) -> float:
        """Most recent ATM IV value (Proxy for India VIX)."""
        return self._current_iv

    @property
    def ivr(self) -> float:
        """
        Computes the IV Rank (0-100) using the rolling 252-day window.
        Formula: (Current - Min) / (Max - Min) * 100
        """
        if not self._history and self._current_iv == 0:
            return 0.0

        # Combine history with the latest live tick
        combined = list(self._history) + [self._current_iv]
        
        low = min(combined)
        high = max(combined)

        if high == low:
            return 50.0 # Neutral if no variance

        rank = ((self._current_iv - low) / (high - low)) * 100
        return round(rank, 2)

    @property
    def regime(self) -> str:
        """
        Returns the volatility regime based on IVR.
        Used to filter entry signals.
        """
        rank = self.ivr
        if rank >= 50:
            return "CREDIT"  # High Vol -> Sell Spreads
        elif rank <= 30:
            return "DEBIT"   # Low Vol -> Buy Spreads (or standby)
        return "NORMAL"

    @property
    def is_ready(self) -> bool:
        """Requires at least some history for a meaningful Rank."""
        return len(self._history) > 30 # Minimum 1 month of context

# Single instance
ivr_engine = IVREngine()
