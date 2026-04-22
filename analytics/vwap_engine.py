from loguru import logger


class VWAPEngine:
    """
    Calculates the cumulative Volume Weighted Average Price
    on the Nifty Futures feed.

    VWAP = Sum(Price × Volume) / Sum(Volume)

    Resets at 09:15 AM every session.
    Updates on every Futures tick — O(1) running sum,
    never recomputes from scratch.

    Why Futures and not Spot?
    Futures have actual traded volume. The Spot index
    is a computed value with no real volume figure.
    VWAP requires genuine traded volume to be meaningful.

    The VWAP value is consumed by Gate 3 of the signal
    engine to confirm directional triggers.
    """

    def __init__(self):
        self._cumulative_pv: float = 0.0  # Sum of Price × ΔVolume
        self._cumulative_vol: float = 0.0  # Sum of ΔVolume
        self._current_vwap: float = 0.0
        self._tick_count: int = 0
        self._last_price: float = 0.0
        self._prev_cumvol: float = 0.0    # Tracks previous cumulative volume

    def reset(self):
        """
        Resets VWAP to zero. Called at 09:15 AM
        at the start of every session.
        """
        self._cumulative_pv = 0.0
        self._cumulative_vol = 0.0
        self._current_vwap = 0.0
        self._tick_count = 0
        self._last_price = 0.0
        self._prev_cumvol = 0.0
        logger.info("[VWAP ENGINE] Reset for new session.")

    def update(self, price: float, cumvol: float) -> float:
        """
        Updates VWAP with a new Futures tick.
        Uses volume-weighting (Price * Volume).
        Detects mid-session restarts by checking if _prev_cumvol is 0;
        in that case, it sets a baseline without affecting vwap,
        preventing the 'giant first tick' error.
        """
        self._last_price = price
        self._tick_count += 1

        if self._prev_cumvol == 0:
            # Baseline capture: don't update VWAP with the full daily volume
            self._prev_cumvol = cumvol
            return self._current_vwap

        delta_vol = cumvol - self._prev_cumvol
        if delta_vol <= 0:
            # Stale tick or out-of-order, skip
            return self._current_vwap

        self._cumulative_pv += price * delta_vol
        self._cumulative_vol += delta_vol
        self._prev_cumvol = cumvol
        self._current_vwap = self._cumulative_pv / self._cumulative_vol
        return self._current_vwap

    @property
    def value(self) -> float:
        """Current VWAP value."""
        return self._current_vwap

    @property
    def last_price(self) -> float:
        """Most recent Futures price."""
        return self._last_price

    @property
    def is_price_above_vwap(self) -> bool:
        """
        True if the last Futures price is above VWAP.
        Used by Gate 3 to track crossover direction.
        """
        if self._current_vwap == 0:
            return False
        return self._last_price > self._current_vwap

    @property
    def is_ready(self) -> bool:
        """
        True once enough ticks have accumulated for
        VWAP to be statistically meaningful.
        Requires at least 100 ticks — roughly 2-3 minutes
        of Futures trading after market open.
        """
        return self._tick_count >= 100

    @property
    def is_extended(self) -> bool:
        """
        True if price is extended beyond the 0.3% V4.1 Rubber-Band limit.
        """
        if self._current_vwap <= 0:
            return False
        distance = abs(self._last_price - self._current_vwap) / self._current_vwap
        return distance > 0.003


# Single instance
vwap_engine = VWAPEngine()
