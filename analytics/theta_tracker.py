"""
analytics/theta_tracker.py
==========================
Profit Harvester — matches backtest_engine.py _check_exits exactly.

Backtest logic:
    basis = abs(net_entry_premium) * LOT_SIZE * lots
    ratio = upnl / basis
    if ratio >= params.profit_target (0.75):  → THETA_HARVEST
    if days >= theta_days (2) and abs(ratio) < theta_min_gain (0.15):  → THETA_STAGNATION
"""

import asyncio
from loguru import logger
from risk.portfolio_state import portfolio_state
from data.market_data_store import market_data_store
from config.constants import THETA_MINIMUM_GAIN_PCT, PROFIT_TARGET, THETA_DAYS


class ThetaTracker:
    def __init__(self):
        self.harvest_threshold_pct = PROFIT_TARGET

    async def sweep(self) -> None:
        if portfolio_state.position_count == 0:
            return

        now_date = None
        try:
            from datetime import datetime

            now_date = datetime.now()
        except Exception:
            return

        for pos_id, pos in list(portfolio_state.open_positions.items()):
            # Update MTM first
            long_data = market_data_store.get_prices(pos.long_strike, pos.option_type)
            short_data = market_data_store.get_prices(pos.short_strike, pos.option_type)
            if not long_data or not short_data:
                continue

            long_mid = (long_data["bid"] + long_data["ask"]) / 2
            short_mid = (short_data["bid"] + short_data["ask"]) / 2
            portfolio_state.update_mark_to_market(
                pos_id, current_premium_long=long_mid, current_premium_short=short_mid
            )

            # --- Backtest basis = abs(net_entry_premium) * lot_size * lots ---
            basis = abs(pos.net_entry_premium) * pos.lot_size * pos.lots
            if basis <= 0:
                continue

            upnl = pos.unrealized_pnl
            ratio = upnl / basis

            # 1. Profit target (matches: if ratio >= params.profit_target)
            if ratio >= PROFIT_TARGET:
                logger.success(
                    f"[THETA TRACKER] HARVEST — {pos_id} captured {ratio*100:.1f}% of credit. Triggering exit."
                )
                asyncio.create_task(self._trigger_exit(pos_id, "THETA_HARVEST"))
                continue

            # 2. Stagnation (matches: if days >= theta_days and abs(ratio) < theta_min_gain)
            days_held = (now_date.date() - pos.entry_timestamp.date()).days
            if days_held >= THETA_DAYS and abs(ratio) < THETA_MINIMUM_GAIN_PCT:
                logger.warning(
                    f"[THETA TRACKER] STAGNATION — {pos_id} held {days_held}d, "
                    f"ratio {ratio*100:.1f}% < {THETA_MINIMUM_GAIN_PCT*100:.0f}%. Exiting."
                )
                asyncio.create_task(self._trigger_exit(pos_id, "THETA_STAGNATION"))

    async def _trigger_exit(self, position_id: str, reason: str):
        from orders.order_manager import order_manager
        from data.market_data_store import market_data_store
        from risk.portfolio_state import portfolio_state

        pos = portfolio_state.open_positions.get(position_id)
        if not pos:
            return

        market_data = (
            market_data_store.get_spread_market_data(
                long_strike=pos.long_strike,
                short_strike=pos.short_strike,
                option_type=pos.option_type,
            )
            or {}
        )

        await order_manager.close_position(
            position_id=position_id,
            market_data=market_data,
            exit_reason=reason,
        )


# Single instance
theta_tracker = ThetaTracker()
