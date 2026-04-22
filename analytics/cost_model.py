from config.constants import (
    STT_RATE,
    SEBI_CHARGE_RATE,
    NSE_TRANSACTION_RATE,
    BROKERAGE_PER_LEG,
    GST_RATE,
    BACKTEST_SLIPPAGE_FLAT,
    BACKTEST_SLIPPAGE_PCT,
)
from loguru import logger


class CostModel:
    """
    Calculates the full transaction cost for any options trade.

    In Ghost Mode: uses live Bid/Ask with dynamic slippage.
    In Backtest mode: uses synthetic slippage on Close price.

    All costs are per LOT. Multiply by number of lots
    for the total cost of a position.

    NOTE: STT, SEBI, and NSE rates were last verified
    against NSE fee schedule in 2024.
    TODO: Verify current rates before live deployment.
    Rates change with NSE/SEBI circulars.
    """

    def calculate_live_slippage(self, bid: float, ask: float) -> float:
        """
        Calculates realistic slippage for a live or Ghost
        Mode fill based on the current Bid-Ask spread width.

        The wider the spread, the higher the slippage penalty.
        A minimum floor prevents unrealistically small penalties
        on illiquid strikes where the spread might be ₹0.10
        but real execution would cost more.
        """
        spread_width = ask - bid
        slippage = max(BACKTEST_SLIPPAGE_FLAT, spread_width * 0.10)
        return round(slippage, 2)

    def calculate_backtest_slippage(self, close_price: float) -> float:
        """
        Synthetic slippage for backtesting where Bid/Ask
        history is unavailable. Uses the worse of a flat
        floor or a percentage of the close price.

        This deliberately overestimates slippage to make
        the backtest conservative rather than optimistic.
        """
        slippage = max(BACKTEST_SLIPPAGE_FLAT, close_price * BACKTEST_SLIPPAGE_PCT)
        return round(slippage, 2)

    def calculate_transaction_costs(
        self, premium: float, lot_size: int, lots: int, is_sell: bool
    ) -> float:
        """
        Calculates the complete transaction cost for one leg
        of a spread order.

        Parameters:
            premium:  Option premium per unit (in ₹)
            lot_size: Number of units per lot (65 for Nifty)
            lots:     Number of lots traded
            is_sell:  True for sell legs (STT applies on sell side)

        Returns total cost in ₹ for this leg.

        Cost components:
        - STT: Securities Transaction Tax — only on sell side
        - SEBI: Turnover charge on both sides
        - NSE: Exchange transaction charge on both sides
        - Brokerage: Flat ₹20 per order leg
        - GST: 18% on brokerage only (not on STT or exchange charges)
        """
        turnover = premium * lot_size * lots

        # STT applies only on the sell side for options
        stt = (turnover * STT_RATE) if is_sell else 0.0

        # SEBI and NSE charges apply on both sides
        sebi_charge = turnover * SEBI_CHARGE_RATE
        nse_charge = turnover * NSE_TRANSACTION_RATE

        # Flat brokerage per leg regardless of size
        brokerage = BROKERAGE_PER_LEG

        # GST only on brokerage — not on exchange charges
        gst = brokerage * GST_RATE

        total = stt + sebi_charge + nse_charge + brokerage + gst

        return round(total, 2)

    def total_spread_cost(
        self,
        long_premium: float,
        short_premium: float,
        lot_size: int,
        lots: int,
        is_entry: bool,
    ) -> float:
        """
        Calculates the total transaction cost for a complete
        spread (both legs together).

        At entry:
          - Long leg is a BUY (no STT)
          - Short leg is a SELL (STT applies)

        At exit:
          - Long leg is a SELL (STT applies)
          - Short leg is a BUY (no STT)

        This asymmetry means entry and exit costs differ
        slightly. Both are calculated correctly here.
        """
        if is_entry:
            long_cost = self.calculate_transaction_costs(
                long_premium, lot_size, lots, is_sell=False
            )
            short_cost = self.calculate_transaction_costs(
                short_premium, lot_size, lots, is_sell=True
            )
        else:
            # At exit the legs reverse
            long_cost = self.calculate_transaction_costs(
                long_premium, lot_size, lots, is_sell=True
            )
            short_cost = self.calculate_transaction_costs(
                short_premium, lot_size, lots, is_sell=False
            )

        total = long_cost + short_cost
        logger.debug(
            f"[COST MODEL] Spread cost ({'entry' if is_entry else 'exit'}): "
            f"Long leg: ₹{long_cost} | "
            f"Short leg: ₹{short_cost} | "
            f"Total: ₹{total}"
        )
        return total


# Single instance
cost_model = CostModel()
