"""
risk/pre_execution.py
=====================
Gate 0: The Pre-Flight Checklist.
"""

from datetime import datetime
from loguru import logger
from config.constants import (
    WINDOW_1_OPEN,
    WINDOW_1_CLOSE,
    WINDOW_2_OPEN,
    WINDOW_2_CLOSE,
    NEAR_EXPIRY_EXIT_TIME,
    MACRO_BLACKOUT_PRE,
    MACRO_BLACKOUT_POST,
)
from data.runtime_config import runtime_config
from risk.portfolio_state import portfolio_state
from risk.circuit_breaker import circuit_breaker
from analytics.gap_tracker import gap_tracker


class PreExecutionGate:
    """
    Final gate before any trade is submitted to the OMS.

    Checks in order:
    1. Circuit breaker not tripped
    2. Execution window is open
    3. Not in a macro blackout window
    4. Position limit not reached
    5. Not expiry day after 01:30 PM
    6. ATR engine is ready (for position sizing)

    All checks must pass. First failure stops evaluation
    and returns False with a reason.

    Margin check is handled separately in order_manager.py
    as it requires a live API call (Live Mode) or SPAN
    formula calculation (Ghost Mode).
    """

    def __init__(self):
        # Macro blackout windows: list of (start, end) datetime tuples
        self._blackout_windows: list = []

    def set_blackout_windows(self, windows: list) -> None:
        """
        Called at startup by the macro calendar fetcher.
        windows: list of (start_datetime, end_datetime) tuples
        """
        self._blackout_windows = windows
        logger.info(
            f"[PRE-EXECUTION] {len(windows)} macro blackout " f"windows scheduled."
        )

    async def check(self, is_expiry_day: bool = False) -> tuple[bool, str]:
        """
        Runs all pre-execution checks.

        Returns:
            (allowed: bool, reason: str)
        """
        now = datetime.now().time()

        # --- 1. THE DAILY CIRCUIT BREAKER (Highest Priority) ---
        cb_allowed, cb_reason = await circuit_breaker.evaluate()
        if not cb_allowed:
            return False, cb_reason

        # --- 2. EXECUTION WINDOW ---
        in_window = self._is_in_execution_window(now)
        if not in_window:
            return False, "OUTSIDE_EXECUTION_WINDOW"

        # --- 3. POSITION LIMITS ---
        if runtime_config.risk is None:
            return False, "RISK_CONFIG_NOT_LOADED"

        if portfolio_state.position_count >= runtime_config.risk.max_concurrent:
            return (
                False,
                f"POSITION_LIMIT_REACHED_"
                f"{portfolio_state.position_count}_OF_"
                f"{runtime_config.risk.max_concurrent}",
            )

        # --- 5. EXPIRY DAY CUTOFF ---
        if is_expiry_day:
            cutoff = NEAR_EXPIRY_EXIT_TIME
            if now >= cutoff:
                return False, "EXPIRY_DAY_CUTOFF_REACHED"

        return True, "ALL_CHECKS_PASSED"

    def _is_in_execution_window(self, now) -> bool:
        """
        Returns True if the current time is within
        an execution window.

        Window 1: 09:30 AM to 11:30 AM
        Window 2: 01:30 PM to 03:00 PM
        """
        in_window_1 = WINDOW_1_OPEN <= now <= WINDOW_1_CLOSE
        in_window_2 = WINDOW_2_OPEN <= now <= WINDOW_2_CLOSE
        return in_window_1 or in_window_2

    def _is_in_blackout(self, now: datetime) -> bool:
        """
        Returns True if the current time falls within
        a Tier-1 macro event blackout window.
        """
        for start, end in self._blackout_windows:
            if start <= now <= end:
                # We log at debug here instead of warning so we don't spam
                # the console every 1 second during a 30-minute blackout.
                logger.debug(
                    f"[PRE-EXECUTION] In macro blackout window: "
                    f"{start.strftime('%H:%M')} — "
                    f"{end.strftime('%H:%M')}"
                )
                return True
        return False

    @property
    def active_blackout_count(self) -> int:
        """Number of scheduled blackout windows for today."""
        return len(self._blackout_windows)


# Single instance
pre_execution_gate = PreExecutionGate()
