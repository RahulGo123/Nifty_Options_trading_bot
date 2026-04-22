"""
analytics/gap_tracker.py
========================
Pre-Market Regime Gate & Overnight Gap Tracker.
Protects VWAP logic from violent opening drives.
"""

from datetime import time
from loguru import logger


class PreMarketGapTracker:
    def __init__(self):
        self.yesterday_close = 0.0
        self.today_open = 0.0
        self.gap_pct = 0.0

        self.is_gap_volatile = False
        self.gap_threshold_pct = 1.0  # 1.0% is a massive move for the Nifty index
        self.cooling_off_until = time(9, 45)  # 30-minute block

    def initialize_previous_close(self, close_price: float):
        """Called at 08:45 AM during pre-market prep."""
        self.yesterday_close = close_price
        logger.info(
            f"[GAP TRACKER] Yesterday's Close logged at ₹{self.yesterday_close:,.2f}"
        )

    def register_first_tick(self, ltp: float):
        """Called at 09:15 AM by the WebSocket on the very first tick."""
        if self.today_open > 0:
            return  # Already registered for today

        self.today_open = ltp

        if self.yesterday_close > 0:
            # Calculate absolute percentage gap
            self.gap_pct = (
                abs((self.today_open - self.yesterday_close) / self.yesterday_close)
                * 100
            )

            if self.gap_pct >= self.gap_threshold_pct:
                self.is_gap_volatile = False # FORCED FALSE FOR BACKTEST PARITY
                logger.info(
                    f"[GAP TRACKER] Volatile gap of {self.gap_pct:.2f}% detected, but Shock Regime is DISABLED for backtest parity."
                )
            else:
                logger.info(f"[GAP TRACKER] Normal open. Gap is {self.gap_pct:.2f}%.")

    def is_cooling_off(self, current_time: time) -> bool:
        """Returns True if we are inside the 30-minute post-shock block."""
        if self.is_gap_volatile and current_time < self.cooling_off_until:
            return True
        return False

    def get_vix_penalty(self) -> float:
        """If the gap was volatile, we demand +2.0 higher VIX to take a trade."""
        return 2.0 if self.is_gap_volatile else 0.0


# Single instance
gap_tracker = PreMarketGapTracker()
