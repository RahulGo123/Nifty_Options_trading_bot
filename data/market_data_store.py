"""
data/market_data_store.py
=========================
Thread-safe memory bank for live option chain data.
"""

from loguru import logger


class MarketDataStore:
    def __init__(self):
        self._data = {}  # Structure: {strike: {"CE": {...}, "PE": {...}}}
        self._indices = {} # Structure: {symbol: ltp}

    def update_index(self, symbol: str, ltp: float):
        self._indices[symbol] = ltp

    def get_index(self, symbol: str) -> float:
        return self._indices.get(symbol, 0.0)

    def update(
        self, strike: int, option_type: str, bid: float, ask: float, oi: int, **kwargs
    ):
        """
        Updates the store. Rejects zero-price 'leaks' and uses a kwargs catch-all
        to prevent TypeErrors from unexpected WebSocket fields (like volume/ltp).
        """
        if strike not in self._data:
            self._data[strike] = {}

        existing = self._data[strike].get(option_type, {})

        # Fall back to existing prices if WebSocket tick has zero bid/ask
        effective_bid = bid if bid > 0 else existing.get("bid", 0.0)
        effective_ask = ask if ask > 0 else existing.get("ask", 0.0)

        # If we still have no price at all, reject the tick
        if effective_bid <= 0 or effective_ask <= 0:
            return

        new_oi = oi if oi > 0 else existing.get("oi", 0)

        updated_dict = dict(existing)
        updated_dict.update({
            "bid": effective_bid,
            "ask": effective_ask,
            "oi": new_oi,
            "mid": (effective_bid + effective_ask) / 2.0,
        })

        for k, v in kwargs.items():
            if isinstance(v, (int, float)) and v <= 0 and k in existing:
                continue
            updated_dict[k] = v

        self._data[strike][option_type] = updated_dict

    def get_mid_price(self, strike: int, option_type: str) -> float:
        """Used for IVR and PCR calculations."""
        return self._data.get(strike, {}).get(option_type, {}).get("mid", 0.0)

    def get_prices(self, strike: int, option_type: str) -> dict:
        """
        REQUIRED BY SCHEDULER: Returns the full data dict for a strike/type.
        """
        return self._data.get(strike, {}).get(option_type, {})

    def get_spread_market_data(
        self, long_strike: int, short_strike: int, option_type: str
    ) -> dict:
        """
        Aggregates market data for both legs of a spread.
        Required by LifecycleManager for calculating exit fills.
        """
        long_data = self.get_prices(long_strike, option_type)
        short_data = self.get_prices(short_strike, option_type)

        if not long_data or not short_data:
            return None

        return {
            "long_bid": long_data.get("bid", 0.0),
            "long_ask": long_data.get("ask", 0.0),
            "short_bid": short_data.get("bid", 0.0),
            "short_ask": short_data.get("ask", 0.0),
        }

    @property
    def tracked_strikes(self):
        return list(self._data.keys())


# Global instance
market_data_store = MarketDataStore()
