import time
from datetime import datetime
from loguru import logger
from config.constants import (
    PCR_SIGNAL_VALIDITY,
    PCR_ACTIVATION_TIME,
)
from data.runtime_config import runtime_config


class PCREngine:
    """
    Calculates the Put-Call Ratio using Change in OI
    for strikes ATM ± 5.

    V4.1 Upgrades:
    - Thresholds shifted to match Total OI recalibrations.
    - 50-day SMA Macro Override (Bidirectional with 1.5% buffer).
    """

    TREND_FILTER_BUFFER_PCT = 0.015  # 1.5% buffer from backtest

    def __init__(self):
        self._current_pcr: float = 0.0
        self._current_signal: str = "NEUTRAL"
        self._signal_generated_at: float = None
        self._is_active: bool = False

        # 50-day SMA of Futures closing prices for macro override
        self._futures_sma_50: float = 0.0
        self._current_spot: float = 0.0

    def set_futures_sma(self, sma_50: float) -> None:
        """
        Sets the 50-day SMA of Nifty Futures closing prices.
        Called at startup from historical database query.
        """
        self._futures_sma_50 = sma_50
        logger.info(f"[PCR ENGINE] Futures 50-day SMA set: {sma_50:.2f}")

    def update_spot(self, spot: float) -> None:
        """
        Updates the current Nifty spot price.
        Used for macro override check.
        """
        self._current_spot = spot

    def activate(self) -> None:
        """
        Called at PCR_ACTIVATION_TIME.
        """
        self._is_active = True
        logger.info(f"[PCR ENGINE] Activated at {PCR_ACTIVATION_TIME.strftime('%H:%M')}.")

    def update(self, oi_delta: dict, atm_strike: int, atm_range: int = 5) -> str:
        """
        Recalculates PCR from the latest OI delta snapshot.
        Returns the current signal: BULLISH, BEARISH, or NEUTRAL.
        """
        if not self._is_active:
            return "NEUTRAL"

        if not oi_delta:
            return self._current_signal

        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            return "NEUTRAL"

        interval = nifty.strike_interval
        total_put = 0
        total_call = 0

        # Sum Change in OI for ATM ± atm_range strikes
        for i in range(-atm_range, atm_range + 1):
            strike = atm_strike + (i * interval)
            if strike in oi_delta:
                total_put += max(0, oi_delta[strike].get("PE", 0))
                total_call += max(0, oi_delta[strike].get("CE", 0))

        # Avoid division by zero
        if total_call == 0:
            self._current_pcr = 0.0
            self._current_signal = "NEUTRAL"
            return "NEUTRAL"

        self._current_pcr = round(total_put / total_call, 4)

        regime = runtime_config.regime
        new_signal = self._classify_signal(
            self._current_pcr, regime.pcr_bullish, regime.pcr_bearish
        )

        # Apply V4.1 Bidirectional Macro Override
        if new_signal != "NEUTRAL" and self._is_macro_blocked(new_signal):
            logger.info(
                f"[PCR ENGINE] {new_signal} signal discarded — "
                f"counter-trend macro override active. "
                f"(Spot: {self._current_spot:.0f}, SMA-50: {self._futures_sma_50:.0f})"
            )
            new_signal = "NEUTRAL"

        # Cancel active signal if direction flips
        if (
            self._current_signal != "NEUTRAL"
            and new_signal != "NEUTRAL"
            and new_signal != self._current_signal
        ):
            logger.info(
                f"[PCR ENGINE] Signal direction flipped "
                f"{self._current_signal} → {new_signal}. "
                f"Previous signal cancelled."
            )
            self._signal_generated_at = None

        # Record signal generation time for validity window
        if new_signal != "NEUTRAL" and new_signal != self._current_signal:
            self._signal_generated_at = time.monotonic()
            logger.info(
                f"[PCR ENGINE] New signal: {new_signal} "
                f"(PCR: {self._current_pcr:.4f})"
            )

        self._current_signal = new_signal
        return self._current_signal

    def _classify_signal(
        self, pcr: float, bullish_threshold: float, bearish_threshold: float
    ) -> str:
        if pcr > bullish_threshold:
            return "BULLISH"
        elif pcr < bearish_threshold:
            return "BEARISH"
        else:
            return "NEUTRAL"

    def _is_macro_blocked(self, signal: str) -> bool:
        """
        Returns True if the signal is counter-trend relative to the 50-day SMA,
        factoring in the 1.5% soft-buffer.
        """
        if self._futures_sma_50 <= 0:
            return False

        buffer = self._futures_sma_50 * self.TREND_FILTER_BUFFER_PCT

        if signal == "BULLISH" and self._current_spot < (self._futures_sma_50 - buffer):
            return True
        if signal == "BEARISH" and self._current_spot > (self._futures_sma_50 + buffer):
            return True

        return False

    def is_signal_valid(self) -> bool:
        """
        Returns True if the current signal is still within
        its validity window (PCR_SIGNAL_VALIDITY minutes).
        """
        if self._current_signal == "NEUTRAL":
            return False
        if self._signal_generated_at is None:
            return False

        elapsed_minutes = (time.monotonic() - self._signal_generated_at) / 60

        if elapsed_minutes > PCR_SIGNAL_VALIDITY:
            logger.info(
                f"[PCR ENGINE] Signal {self._current_signal} expired "
                f"after {elapsed_minutes:.1f} minutes."
            )
            self._current_signal = "NEUTRAL"
            self._signal_generated_at = None
            return False

        return True

    @property
    def pcr(self) -> float:
        return self._current_pcr

    async def update_macro_sma(self) -> None:
        """
        Placeholder for end-of-day Macro SMA calculation.
        Currently stubbed out until daily historical price/SMA tracking is implemented in the DB.
        """
        pass

    @property
    def signal(self) -> str:
        return self._current_signal


# Single instance
pcr_engine = PCREngine()
