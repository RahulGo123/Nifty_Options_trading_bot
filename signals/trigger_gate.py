"""
signals/trigger_gate.py
=======================
Gate 3: VWAP Proximity Trigger.

Matches the backtest's vwap_pullback_tolerance check exactly:

    vwap_distance = abs(fut_c - current_vwap) / current_vwap
    if vwap_distance > params.vwap_pullback_tolerance:
        continue  # skip — price too far from VWAP

The trigger fires when:
  - VWAP engine is ready (has processed >= 1 tick)
  - Price is within 0.45% of the current session VWAP
  - This condition has persisted for >= VWAP_CONFIRMATION_CLOSES 1-minute candles

Direction is ignored — the backtest is delta-neutral (Iron Condor).
"""

from typing import NamedTuple
from loguru import logger
from analytics.vwap_engine import vwap_engine
from config.constants import VWAP_CONFIRMATION_CLOSES, VWAP_PULLBACK_TOLERANCE


class TriggerResult(NamedTuple):
    confirmed: bool


class TriggerGate:
    def __init__(self):
        self._triggered: bool = False
        self._confirmation_count: int = 0
        self._last_candle_minute: int = -1

    def evaluate(self, direction: str = "NEUTRAL") -> TriggerResult:
        return TriggerResult(confirmed=self._triggered)

    def on_futures_tick(self, price: float, timestamp, direction: str = "NEUTRAL") -> bool:
        try:
            from analytics.gap_tracker import gap_tracker
            gap_tracker.register_first_tick(price)
        except Exception:
            pass

        if not vwap_engine.is_ready:
            return False

        minute = timestamp.minute
        if minute != self._last_candle_minute:
            # 1-minute candle close — check VWAP proximity (matches backtest)
            vwap = vwap_engine.value
            if vwap <= 0:
                self._last_candle_minute = minute
                return False

            vwap_distance = abs(price - vwap) / vwap

            if vwap_distance <= VWAP_PULLBACK_TOLERANCE:
                self._confirmation_count += 1
            else:
                self._confirmation_count = 0  # Price moved too far — reset

            if self._confirmation_count >= VWAP_CONFIRMATION_CLOSES:
                if not self._triggered:
                    logger.info(
                        f"[TRIGGER GATE] VWAP proximity confirmed at {price:.2f} "
                        f"(VWAP: {vwap:.2f}, Dist: {vwap_distance*100:.3f}%)"
                    )
                    self._triggered = True
            else:
                logger.debug(
                    f"[TRIGGER GATE] Proximity count: {self._confirmation_count}/{VWAP_CONFIRMATION_CLOSES} "
                    f"| Price: {price:.2f} | VWAP dist: {vwap_distance*100:.3f}%"
                )

            self._last_candle_minute = minute

        return self._triggered

    def reset(self) -> None:
        self._triggered = False
        self._confirmation_count = 0
        self._last_candle_minute = -1
        logger.info("[TRIGGER GATE] Full reset.")


trigger_gate = TriggerGate()
