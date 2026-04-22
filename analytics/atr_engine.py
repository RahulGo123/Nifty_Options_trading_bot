from collections import deque
from datetime import datetime
from loguru import logger


class ATREngine:
    """
    Computes a 14-period Average True Range on 15-minute
    Futures candles built from the live tick feed.

    ATR measures the average price range the market covers
    in a single candle. The position sizer uses it to
    calculate the stop-loss distance:

        Stop distance = 1.5 × ATR (in index points)
        Lots = floor(₹25,000 / (stop_distance × 65))

    A larger ATR means a wider stop and fewer lots.
    A smaller ATR means a tighter stop and more lots.
    This automatically adjusts position size to match
    current market volatility — a core property of
    fixed fractional position sizing.

    Why 15-minute candles?
    Shorter candles (1-minute) produce ATR values that
    are too noisy — a single spike doubles the ATR.
    15-minute candles smooth out intraday noise while
    still capturing the day's genuine volatility range.
    """

    CANDLE_MINUTES = 15
    ATR_PERIOD = 14

    def __init__(self):
        # Stores completed candles as (open, high, low, close)
        self._candles: deque = deque(maxlen=self.ATR_PERIOD + 1)
        self._current_candle: dict = None
        self._current_atr: float = 0.0
        self._candle_count: int = 0
        self._is_seeded: bool = False

    def seed_candles(self, candles: list[dict]) -> None:
        """
        Pre-populates the engine with historical candles.
        Expected format: list of {'open', 'high', 'low', 'close'}
        """
        if not candles:
            return

        for candle in candles:
            self._candles.append(candle)
            self._candle_count += 1
            self._recalculate_atr()

        self._is_seeded = True
        logger.info(f"[ATR ENGINE] Successfully seeded with {len(candles)} historical candles. ATR-14: {self._current_atr:.2f}")

    def update(self, price: float, timestamp: datetime) -> None:
        """
        Processes a new Futures tick.
        Builds 15-minute candles and recalculates ATR
        when each candle closes.
        """
        candle_slot = self._get_candle_slot(timestamp)

        if self._current_candle is None:
            # First tick of the session — open the first candle
            self._current_candle = self._new_candle(price, candle_slot)
            return

        if candle_slot == self._current_candle["slot"]:
            # Same candle — update high, low, close
            self._current_candle["high"] = max(self._current_candle["high"], price)
            self._current_candle["low"] = min(self._current_candle["low"], price)
            self._current_candle["close"] = price

        else:
            # New candle — close the current one and start fresh
            self._candles.append(self._current_candle)
            self._candle_count += 1
            self._recalculate_atr()
            self._current_candle = self._new_candle(price, candle_slot)

    def _get_candle_slot(self, timestamp: datetime) -> int:
        """
        Maps a timestamp to a candle slot number.
        All ticks within the same 15-minute window
        get the same slot number.

        Example: 09:15, 09:16 ... 09:29 → slot 0
                 09:30, 09:31 ... 09:44 → slot 1
        """
        minutes_since_open = (timestamp.hour - 9) * 60 + (timestamp.minute - 15)
        return minutes_since_open // self.CANDLE_MINUTES

    def _new_candle(self, price: float, slot: int) -> dict:
        """Creates a new candle with the opening price."""
        return {
            "slot": slot,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }

    def _recalculate_atr(self) -> None:
        """
        Recalculates ATR from completed candles.
        Uses Wilder's smoothing method — the standard
        for ATR as defined by J. Welles Wilder.

        True Range for each candle is the largest of:
          1. High - Low (the candle's own range)
          2. |High - Previous Close| (gap up scenario)
          3. |Low - Previous Close|  (gap down scenario)

        Taking the maximum of these three captures both
        intraday range AND overnight/between-candle gaps.
        """
        candles = list(self._candles)
        if len(candles) < 2:
            return

        true_ranges = []
        for i in range(1, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i - 1]["close"]

            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)

        if not true_ranges:
            return

        # Simple average for the first ATR value
        if len(true_ranges) < self.ATR_PERIOD:
            self._current_atr = sum(true_ranges) / len(true_ranges)
        else:
            # Wilder's smoothing: new ATR = (prev ATR × 13 + TR) / 14
            prev_atr = (
                self._current_atr
                if self._current_atr > 0
                else sum(true_ranges[: self.ATR_PERIOD]) / self.ATR_PERIOD
            )
            latest_tr = true_ranges[-1]
            self._current_atr = (
                prev_atr * (self.ATR_PERIOD - 1) + latest_tr
            ) / self.ATR_PERIOD

        logger.debug(
            f"[ATR ENGINE] Candle {self._candle_count} closed. "
            f"ATR-14: {self._current_atr:.2f} points."
        )

    @property
    def value(self) -> float:
        """Current ATR-14 value in index points. Fallback 60.0 for safety."""
        return self._current_atr if self._current_atr > 0 else 60.0

    @property
    def atr(self) -> float:
        """Alias for backward compatibility. Fallback 60.0 for safety."""
        return self._current_atr if self._current_atr > 0 else 60.0

    @property
    def is_ready(self) -> bool:
        """Always True: falls back to 60.0 if live data is missing."""
        return True


# Single instance
atr_engine = ATREngine()
