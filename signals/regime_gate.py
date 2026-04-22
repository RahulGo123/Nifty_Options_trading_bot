"""
signals/regime_gate.py
======================
Gate 1: VIX Range Filter — matches backtest_engine.py exactly.

The backtest check (line 448):
    if (iv * 100) < params.min_vix or (iv * 100) > params.max_vix:
        continue

Locked params (run_true_chronological.py):
    min_vix = 11.0
    max_vix = 22.0

ivr_engine.current_iv holds the raw India VIX value (e.g. 14.5 = 14.5%).
No other regime check exists in the backtest — just this range.

The gap cooling-off period (09:15–09:45 on gap day) is handled by
pre_execution_gate, which matches the backtest's excluded_days behaviour.
"""

from typing import NamedTuple
from loguru import logger
from analytics.ivr_engine import ivr_engine
from config.constants import VIX_MIN, VIX_MAX


class RegimeResult(NamedTuple):
    allowed: bool
    regime: str
    spread_type: str | None


class RegimeGate:
    def __init__(self):
        self._last_logged_vix: float = -1.0

    def evaluate(self) -> RegimeResult:
        current_vix = ivr_engine.current_iv  # India VIX, e.g. 14.5

        # Exact backtest condition: (iv * 100) < min_vix or (iv * 100) > max_vix
        if current_vix < VIX_MIN or current_vix > VIX_MAX:
            if abs(current_vix - self._last_logged_vix) >= 0.5:
                logger.info(
                    f"[REGIME GATE] BLOCKED — VIX {current_vix:.1f} outside [{VIX_MIN}, {VIX_MAX}]"
                )
                self._last_logged_vix = current_vix
            return RegimeResult(False, "STANDBY", None)

        if abs(current_vix - self._last_logged_vix) >= 0.5:
            logger.info(f"[REGIME GATE] PASS — VIX {current_vix:.1f} in [{VIX_MIN}, {VIX_MAX}]")
            self._last_logged_vix = current_vix

        return RegimeResult(True, "CREDIT", "IRON_CONDOR")


regime_gate = RegimeGate()
