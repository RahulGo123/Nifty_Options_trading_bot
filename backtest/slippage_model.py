"""
backtest/slippage_model.py
==========================
Budget 2026 Exact Tax Matrix & Synthetic Slippage
Calibrated specifically for DhanHQ Flat Fee Structure.
"""

from __future__ import annotations
from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Synthetic slippage constants
# ---------------------------------------------------------------------------
BACKTEST_SLIPPAGE_FLAT = 0.50
BACKTEST_SLIPPAGE_PCT = 0.005

# ---------------------------------------------------------------------------
# 2026 Indian Tax & Fee Constants
# ---------------------------------------------------------------------------
STT_RATE = 0.0015  # 0.15% on sell side premium
STAMP_DUTY_RATE = 0.00003  # 0.003% on buy side premium
NSE_TRANSACTION_RATE = 0.0003503  # 0.03503% of turnover
SEBI_CHARGE_RATE = 0.000001  # 0.0001% of turnover
IPFT_CHARGE_RATE = 0.000001  # 0.0001% of turnover
GST_RATE = 0.18  # 18% GST

# DHAN SPECIFIC BROKERAGE (Flat ₹20 per executed leg, NO lot multiplier)
BROKERAGE_PER_LEG = 20.0


def buy_fill_price(close: float) -> float:
    slippage = max(BACKTEST_SLIPPAGE_FLAT, close * BACKTEST_SLIPPAGE_PCT)
    return round(close + slippage, 2)


def sell_fill_price(close: float) -> float:
    slippage = max(BACKTEST_SLIPPAGE_FLAT, close * BACKTEST_SLIPPAGE_PCT)
    return round(close - slippage, 2)


def slippage_amount(close: float) -> float:
    return max(BACKTEST_SLIPPAGE_FLAT, close * BACKTEST_SLIPPAGE_PCT)


@dataclass
class CostBreakdown:
    stt: float = 0.0
    stamp: float = 0.0
    sebi: float = 0.0
    ipft: float = 0.0
    nse: float = 0.0
    brokerage: float = 0.0
    gst: float = 0.0

    @property
    def total(self) -> float:
        return (
            self.stt
            + self.stamp
            + self.sebi
            + self.ipft
            + self.nse
            + self.brokerage
            + self.gst
        )


def transaction_costs(
    long_premium: float, short_premium: float, lot_size: int, lots: int, is_entry: bool
) -> CostBreakdown:
    contracts = lot_size * lots

    long_turnover = long_premium * contracts
    short_turnover = short_premium * contracts
    total_turnover = long_turnover + short_turnover

    if is_entry:
        buy_turnover = long_turnover  # We buy the long leg
        sell_turnover = short_turnover  # We sell the short leg
    else:
        buy_turnover = short_turnover  # We buy back the short leg
        sell_turnover = long_turnover  # We sell back the long leg

    stt = sell_turnover * STT_RATE
    stamp = buy_turnover * STAMP_DUTY_RATE
    sebi = total_turnover * SEBI_CHARGE_RATE
    ipft = total_turnover * IPFT_CHARGE_RATE
    nse = total_turnover * NSE_TRANSACTION_RATE

    # CRITICAL DHAN LOGIC: Brokerage is strictly 2 * ₹20 = ₹40 total per spread entry/exit.
    # It does NOT multiply by the `lots` argument.
    brokerage = BROKERAGE_PER_LEG * 2

    # GST only applies to Brokerage, NSE, and SEBI charges.
    gst = (brokerage + nse + sebi) * GST_RATE

    return CostBreakdown(
        stt=round(stt, 4),
        stamp=round(stamp, 4),
        sebi=round(sebi, 4),
        ipft=round(ipft, 4),
        nse=round(nse, 4),
        brokerage=round(brokerage, 2),
        gst=round(gst, 4),
    )


def total_spread_cost(
    long_premium: float, short_premium: float, lot_size: int, lots: int, is_entry: bool
) -> float:
    return transaction_costs(
        long_premium, short_premium, lot_size, lots, is_entry
    ).total


@dataclass
class SpreadFill:
    long_fill: float
    short_fill: float
    costs: float
    slippage: float

    @property
    def net_premium(self) -> float:
        """Net cash flow at fill. (Negative means debit paid, positive means credit received)."""
        return self.short_fill - self.long_fill


def compute_entry_fill(
    long_close: float, short_close: float, lot_size: int, lots: int
) -> SpreadFill:
    long_fill = buy_fill_price(long_close)
    short_fill = sell_fill_price(short_close)
    costs = total_spread_cost(long_fill, short_fill, lot_size, lots, is_entry=True)
    slip = slippage_amount(long_close) + slippage_amount(short_close)
    return SpreadFill(long_fill, short_fill, costs, slip)


def compute_exit_fill(
    long_close: float, short_close: float, lot_size: int, lots: int
) -> SpreadFill:
    long_fill = sell_fill_price(long_close)
    short_fill = buy_fill_price(short_close)
    costs = total_spread_cost(long_fill, short_fill, lot_size, lots, is_entry=False)
    slip = slippage_amount(long_close) + slippage_amount(short_close)
    return SpreadFill(long_fill, short_fill, costs, slip)
