"""
signals/bias_gate.py
====================
Gate 2: Directional Bias with Warm-up Visibility.
"""

from typing import NamedTuple, Optional
from loguru import logger
from analytics.pcr_engine import pcr_engine


class BiasResult(NamedTuple):
    allowed: bool
    direction: str
    spread_type: Optional[str]


class BiasGate:
    def __init__(self):
        self._last_logged_state: str | None = None

    def evaluate(self, regime: str, pcr_active: bool = False) -> BiasResult:
        """
        Determines directional bias. Includes a warm-up log for transparency.
        """
        # 1. Check if the engine is even active
        if not pcr_active:
            return BiasResult(False, "NEUTRAL", None)

        # 2. Check for the true 'Warm Up' (First 60 seconds after bot start)
        # PCR is 0.0 only when no delta snapshots have been processed yet.
        if pcr_engine.pcr == 0.0:
            state_str = "WARMING_UP"
            if state_str != self._last_logged_state:
                logger.info("[BIAS GATE] Warm-up: Collecting first Open Interest snapshots...")
                self._last_logged_state = state_str
            return BiasResult(False, "NEUTRAL", None)

        direction = pcr_engine.signal
        pcr_val = pcr_engine.pcr
        spread_structure = self._resolve_spread_structure(direction, regime)

        # 3. Handle Neutral state (Data is valid, but no signal yet)
        if direction == "NEUTRAL":
            state_str = f"NEUTRAL_{pcr_val:.2f}"
            if state_str != self._last_logged_state:
                logger.info(f"[BIAS GATE] Neutral — PCR: {pcr_val:.4f} (Waiting for conviction)")
                self._last_logged_state = state_str
            return BiasResult(False, "NEUTRAL", None)

        # 4. Handle Active Signal
        state_str = f"{direction}_{spread_structure}_{pcr_val:.1f}"
        if state_str != self._last_logged_state:
            logger.info(f"[BIAS GATE] PASS — {direction} (PCR: {pcr_val:.4f}) | Plan: {spread_structure}")
            self._last_logged_state = state_str

        return BiasResult(True, direction, spread_structure)

    def _resolve_spread_structure(self, direction: str, regime: str) -> Optional[str]:
        """
        BULLISH + CREDIT = Sell Puts (Bull Put Spread)
        BEARISH + CREDIT = Sell Calls (Bear Call Spread)
        """
        mapping = {
            ("CREDIT", "BULLISH"): "BULL_PUT_SPREAD",
            ("CREDIT", "BEARISH"): "BEAR_CALL_SPREAD",
            ("DEBIT", "BULLISH"): "BULL_CALL_SPREAD",
            ("DEBIT", "BEARISH"): "BEAR_PUT_SPREAD",
        }
        return mapping.get((regime, direction))


bias_gate = BiasGate()
