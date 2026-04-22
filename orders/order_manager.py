"""
orders/order_manager.py
=======================
DhanHQ Live Execution Orchestrator.
Routes order execution to either the Ghost Ledger or the Dhan API.
Enforces strict execution sequencing for SPAN margin optimization.
"""

import asyncio
import aiohttp
from loguru import logger
from config.settings import settings
from orders.ghost_ledger import ghost_ledger
from risk.portfolio_state import portfolio_state
from data.runtime_config import runtime_config
from monitoring.alerting import send_alert
from execution.margin_cache import margin_cache
from risk.reconciliation import reconciliation_engine
from analytics.trade_logger import trade_logger
from analytics.ivr_engine import ivr_engine
from analytics.greeks_engine import greeks_engine


class OrderManager:
    """
    Top-level order orchestrator.
    Routes order execution to either:
        Ghost Ledger  (LIVE_TRADING = False)
        Broker API    (LIVE_TRADING = True)
    """

    def __init__(self):
        self._last_filter_state = None

    async def submit_basket(
        self,
        spread_type: str,
        atm_strike: int,
        market_data: dict,
    ) -> tuple[bool, str]:
        """
        The Master Entry Gateway.
        """
        from orders.basket_builder import basket_builder
        from execution.margin_cache import margin_cache
        from data.runtime_config import runtime_config

        from risk.position_sizer import position_sizer

        lots, stop_dist, stop_value = position_sizer.calculate(spread_type)

        if lots < 1:
            state_key = f"INSUFFICIENT_{spread_type}_{atm_strike}"
            if state_key != self._last_filter_state:
                logger.info(
                    "[ORDER MANAGER] Filter: Budget full or margin insufficient for new entries."
                )
                self._last_filter_state = state_key
            return False, "INSUFFICIENT_CAPITAL"

        # Signal passed filters — reset the suppression state
        self._last_filter_state = None

        basket = basket_builder.build(spread_type, atm_strike, lots)
        if not basket:
            logger.warning("[ORDER MANAGER] Basket builder returned None. Skipping.")
            return False, "BASKET_BUILD_FAILED"

        from risk.correlation import correlation_guard

        corr_passed, corr_reason = correlation_guard.evaluate(basket)
        if not corr_passed:
            return False, corr_reason

        if not self._validate_market_data(basket, market_data):
            return False, "SENTINEL_LIQUIDITY_REJECTION"

        # 1. Find the worst Bid/Ask spread % in the basket to track execution friction
        max_spread_pct = 0.0
        for leg in basket["legs"]:
            strike = leg["strike"]
            opt_type = leg["option_type"]
            bid = market_data[strike][opt_type]["bid"]
            ask = market_data[strike][opt_type]["ask"]
            if bid > 0:
                spread_pct = ((ask - bid) / bid) * 100
                max_spread_pct = max(max_spread_pct, spread_pct)

        # 2. Package the Greeks & Regime State
        entry_analytics = {
            "ivr_at_entry": ivr_engine.ivr,
            "vix_at_entry": ivr_engine.current_iv,
            "nifty_spot": market_data.get("spot", 0.0),
            "portfolio_delta": 0.0,
            "portfolio_theta": 0.0,
            "max_bid_ask_spread_pct": round(max_spread_pct, 2),
        }

        position_id = portfolio_state.generate_position_id()

        # 3. Choose Execution Path
        if settings.live_trading:
            success = await self._submit_live_basket(
                basket,
                position_id,
                stop_dist=stop_dist,
                entry_analytics=entry_analytics,
            )
        else:
            success = await ghost_ledger.fill_basket(
                basket=basket,
                market_data=market_data,
                position_id=position_id,
                stop_loss_pts=stop_dist,
                entry_analytics=entry_analytics,
            )

        if success:
            # --- STABILITY FIX: Safe Alerts ---
            try:
                from execution.margin_cache import margin_cache

                live_margin = margin_cache.get()
                
                # HEDGED MARGIN LOGIC: In an Iron Condor, 1 lot of Put + 1 lot of Call = 1 lot of Margin.
                put_lots = sum(p.lots for p in portfolio_state.open_positions.values() if "PUT" in p.spread_type)
                call_lots = sum(p.lots for p in portfolio_state.open_positions.values() if "CALL" in p.spread_type)
                
                margin_lots = max(put_lots, call_lots)
                free_cap = runtime_config.risk.total_capital - (margin_lots * live_margin)

                buys = [l for l in basket["legs"] if l["action"] == "BUY"]
                sells = [l for l in basket["legs"] if l["action"] == "SELL"]
                strikes_desc = (
                    f"{buys[0]['strike']}{buys[0]['option_type']} / {sells[0]['strike']}{sells[0]['option_type']}"
                    if buys and sells
                    else "Spread"
                )

                asyncio.create_task(
                    send_alert(
                        f"🚀 <b>ENTRY EXECUTED</b>\n"
                        f"<b>ID:</b> <code>{position_id}</code>\n"
                        f"<b>Type:</b> {basket['spread_type']}\n"
                        f"<b>Strikes:</b> <code>{strikes_desc}</code>\n"
                        f"<b>Lots:</b> {basket['lots']}\n"
                        f"💰 <b>Free Capital:</b> <code>₹{free_cap:,.2f}</code>"
                    )
                )
            except Exception as alert_err:
                logger.error(
                    f"[ORDER MANAGER] Alert logic failed but trade is safe: {alert_err}"
                )

            return True, "EXECUTED"

        logger.warning(f"[ORDER MANAGER] Fill failed for {position_id}")
        return False, "EXECUTION_FAILED"

    async def _submit_live_basket(
        self,
        basket: dict,
        position_id: str,
        stop_dist: float,
        entry_analytics: dict = None,
    ) -> bool:
        """
        Executes a Multi-Leg Basket Order via Dhan API.
        Enforces strict BUY-FIRST sequence to secure SPAN margin.
        """
        from auth.dhan_auth import auth_engine

        url = "https://api.dhan.co/v2/orders"
        headers = auth_engine.get_headers()

        buy_legs = [leg for leg in basket["legs"] if leg.get("action") == "BUY"]
        sell_legs = [leg for leg in basket["legs"] if leg.get("action") == "SELL"]
        ordered_legs = buy_legs + sell_legs

        async with aiohttp.ClientSession() as session:
            for leg in ordered_legs:
                quantity = int(leg["lots"] * basket["lot_size"])
                dhan_transaction_type = "BUY" if leg["action"] == "BUY" else "SELL"

                payload = {
                    "dhanClientId": auth_engine.client_id,
                    "correlationId": f"{position_id}_{leg['strike']}{leg['option_type']}",
                    "transactionType": dhan_transaction_type,
                    "exchangeSegment": "NSE_FNO",
                    "productType": "NRML",
                    "orderType": "MARKET",
                    "validity": "DAY",
                    "securityId": str(leg["token"]),
                    "quantity": quantity,
                    "disclosedQuantity": 0,
                    "price": 0,
                    "triggerPrice": 0,
                    "afterMarketOrder": False,
                    "amoTime": "OPEN",
                    "boProfitValue": 0,
                    "boStopLossValue": 0,
                }

                try:
                    async with session.post(
                        url, json=payload, headers=headers
                    ) as response:
                        data = await response.json()

                        if response.status == 200 and data.get("orderStatus") in [
                            "TRADED",
                            "PENDING",
                        ]:
                            logger.info(
                                f"[ORDER MANAGER] {dhan_transaction_type} {quantity}x {leg['strike']}{leg['option_type']} "
                                f"executed. Ref: {data.get('orderId')}"
                            )
                        else:
                            error_msg = data.get(
                                "internalErrorCode", "Unknown Dhan API Error"
                            )
                            remarks = data.get("remarks", "")
                            combined_error = f"{error_msg} | {remarks}".lower()

                            logger.error(
                                f"[ORDER MANAGER] Dhan Leg Rejected: {combined_error} | Payload: {payload}"
                            )

                            if (
                                "margin" in combined_error
                                or "insufficient" in combined_error
                            ):
                                logger.warning(
                                    f"[ORDER MANAGER] Margin rejection detected on {dhan_transaction_type} leg."
                                )
                                if margin_cache.escalate_margin():
                                    logger.info(
                                        "[ORDER MANAGER] Margin tier escalated. Next signal tick will recalculate sizing."
                                    )
                                else:
                                    logger.critical(
                                        "[ORDER MANAGER] Margin exhausted at max tier."
                                    )

                            await send_alert(
                                f"HIGH: Broker Rejected Leg {position_id} — {error_msg} | {remarks}"
                            )

                            if dhan_transaction_type == "BUY":
                                logger.critical(
                                    "[ORDER MANAGER] BUY leg failed. Aborting remaining basket execution!"
                                )
                                await send_alert(
                                    "CRITICAL: BUY leg failed. Basket execution aborted to prevent naked shorts."
                                )
                                return False

                            return False

                except Exception as e:
                    logger.error(f"[ORDER MANAGER] Network failure submitting leg: {e}")
                    await send_alert(f"CRITICAL: Network failure on execution — {e}")
                    return False

        logger.success(
            f"[ORDER MANAGER] Iron Condor {position_id} completely deployed."
        )

        basket_type = basket.get("type", "Iron Condor")
        contracts_text = ""
        for leg in basket.get("legs", []):
            action = leg.get("action", "UNK")
            strike = leg.get("strike", "0")
            opt = leg.get("option_type", "")
            lots = leg.get("lots", 0)
            contracts_text += f"• {action} {lots}x {strike}{opt}\n"

        alert_msg = (
            f"🟢 <b>ENTRY EXECUTED</b>\n"
            f"<b>ID:</b> <code>{position_id}</code>\n"
            f"<b>Type:</b> {basket_type}\n"
            f"<b>Contracts:</b>\n{contracts_text}"
            f"💰 <b>Free Capital:</b> <code>₹{runtime_config.risk.total_capital - (sum(p.lots for p in portfolio_state.open_positions.values()) * margin_cache.get()):,.2f}</code>\n"
            f"<b>Status:</b> All legs successfully filled on Dhan."
        )
        await send_alert(alert_msg)

        # --- EVENT-DRIVEN POST-ENTRY RECONCILIATION ---
        async def delayed_entry_audit():
            await asyncio.sleep(3.0)
            await reconciliation_engine.run_reconciliation()

        asyncio.create_task(delayed_entry_audit(), name="post_entry_audit")

        return True

    async def close_position(
        self,
        position_id: str,
        market_data: dict,
        exit_reason: str,
    ) -> bool:
        """Closes an open position."""
        if not settings.live_trading:
            success = await ghost_ledger.close_basket(
                position_id=position_id,
                market_data=market_data,
                exit_reason=exit_reason,
            )
        else:
            success = await self._close_live_position(
                position_id=position_id,
                exit_reason=exit_reason,
            )

        if success:
            closed_pos = portfolio_state.get_closed_position(position_id)

            if closed_pos:
                trade_logger.log_trade(
                    position_id=position_id,
                    position_obj=closed_pos,
                    exit_reason=exit_reason,
                )
            asyncio.create_task(
                self._send_delayed_exit_alert(position_id, exit_reason),
                name="delayed_exit_alert",
            )

            async def delayed_exit_audit():
                await asyncio.sleep(3.0)
                await reconciliation_engine.run_reconciliation()

            asyncio.create_task(delayed_exit_audit(), name="post_exit_audit")

        return success

    async def _send_delayed_exit_alert(self, position_id: str, exit_reason: str):
        """Background worker that waits 5 seconds for fills, fetches P&L, and alerts."""
        await asyncio.sleep(5)

        closed_position = portfolio_state.get_closed_position(position_id)
        pnl = getattr(closed_position, "realized_pnl", 0.0) if closed_position else 0.0

        reason_formatted = exit_reason.replace("_", " ").upper()
        icon = "🔴"
        if "PROFIT" in reason_formatted:
            icon = "🎯"
        elif "LOSS" in reason_formatted:
            icon = "🛑"
        elif "EXPIRY" in reason_formatted or "END" in reason_formatted:
            icon = "⏱️"

        if pnl > 0:
            pnl_str = f"🟢 +₹{pnl:,.2f}"
        elif pnl < 0:
            pnl_str = f"🔴 -₹{abs(pnl):,.2f}"
        else:
            pnl_str = f"⚪ ₹0.00"

        alert_msg = (
            f"{icon} <b>POSITION CLOSED</b>\n"
            f"<b>ID:</b> <code>{position_id}</code>\n"
            f"<b>Exit Reason:</b> {reason_formatted}\n"
            f"<b>Net P&L:</b> {pnl_str}\n"
            f"💰 <b>Final Free Capital:</b> <code>₹{runtime_config.risk.total_capital - (sum(p.lots for p in portfolio_state.open_positions.values()) * margin_cache.get()):,.2f}</code>\n"
            f"<b>Status:</b> All legs successfully exited."
        )
        await send_alert(alert_msg)

    async def _check_margin(
        self, lots: int, spread_type: str, atm_strike: int
    ) -> tuple:
        if runtime_config.risk is None:
            return False, "RISK_CONFIG_NOT_LOADED"

        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            return False, "INSTRUMENT_CONFIG_NOT_LOADED"

        margin_per_condor = margin_cache.get()
        required = margin_per_condor * lots * 1.15

        open_count = portfolio_state.position_count
        deployed = open_count * margin_per_condor
        available = runtime_config.risk.total_capital - deployed

        if available < required:
            return (
                False,
                f"INSUFFICIENT_MARGIN_available=₹{available:,.0f}_required=₹{required:,.0f}",
            )

        return True, "MARGIN_OK"

    def _validate_market_data(self, basket: dict, market_data: dict) -> bool:
        """
        Matches backtest _open_position check:
            lc = option_closes.get((long_s, otype))
            sc = option_closes.get((short_s, otype))
            if not lc or not sc or lc <= 0 or sc <= 0: return
        Only rejects if price is zero or missing. No spread-width filter.
        """
        for leg in basket["legs"]:
            strike = leg["strike"]
            option_type = leg["option_type"]

            strike_data = market_data.get(strike)
            if not strike_data:
                return False

            leg_data = strike_data.get(option_type)
            if not leg_data:
                return False

            bid = leg_data.get("bid", 0.0)
            ask = leg_data.get("ask", 0.0)

            if bid <= 0 or ask <= 0:
                return False

        return True

    async def _close_live_position(self, position_id: str, exit_reason: str) -> bool:
        """
        Executes the live exit of a position on Dhan.
        CRITICAL SEQUENCING: Buys back SHORT legs FIRST, then Sells LONG legs.
        """
        from auth.dhan_auth import auth_engine

        pos = portfolio_state.open_positions.get(position_id)
        if not pos:
            logger.error(
                f"[ORDER MANAGER] Cannot close {position_id} — not found in portfolio."
            )
            return False

        url = "https://api.dhan.co/v2/orders"
        headers = auth_engine.get_headers()

        buy_to_close_legs = [leg for leg in pos.legs if leg["action"] == "SELL"]
        sell_to_close_legs = [leg for leg in pos.legs if leg["action"] == "BUY"]
        ordered_exit_legs = buy_to_close_legs + sell_to_close_legs

        async with aiohttp.ClientSession() as session:
            for leg in ordered_exit_legs:
                exit_action = "BUY" if leg["action"] == "SELL" else "SELL"
                quantity = int(leg["lots"] * pos.lot_size)
                correlation_id = (
                    f"{position_id}_{leg['strike']}{leg['option_type']}_EXIT"
                )

                payload = {
                    "dhanClientId": auth_engine.client_id,
                    "correlationId": correlation_id,
                    "transactionType": exit_action,
                    "exchangeSegment": "NSE_FNO",
                    "productType": "NRML",
                    "orderType": "MARKET",
                    "validity": "DAY",
                    "securityId": str(leg["token"]),
                    "quantity": quantity,
                    "disclosedQuantity": 0,
                    "price": 0,
                    "triggerPrice": 0,
                    "afterMarketOrder": False,
                    "amoTime": "OPEN",
                    "boProfitValue": 0,
                    "boStopLossValue": 0,
                }

                try:
                    async with session.post(
                        url, json=payload, headers=headers
                    ) as response:
                        data = await response.json()

                        if response.status == 200 and data.get("orderStatus") in [
                            "TRADED",
                            "PENDING",
                        ]:
                            logger.info(
                                f"[ORDER MANAGER] EXIT: {exit_action} {quantity}x {leg['strike']}{leg['option_type']} "
                                f"executed. Ref: {data.get('orderId')}"
                            )
                        else:
                            error_msg = data.get(
                                "internalErrorCode", "Unknown Dhan API Error"
                            )
                            remarks = data.get("remarks", "")
                            logger.error(
                                f"[ORDER MANAGER] Dhan Exit Leg Rejected: {error_msg} | {remarks}"
                            )

                            await send_alert(
                                f"🚨 <b>CRITICAL EXIT FAILURE</b>\n"
                                f"<b>ID:</b> <code>{position_id}</code>\n"
                                f"<b>Failed Leg:</b> {exit_action} {leg['strike']}{leg['option_type']}\n"
                                f"<b>Reason:</b> {error_msg}\n"
                                f"<b>Action Required:</b> Manual intervention to flatten position!"
                            )

                except Exception as e:
                    logger.error(
                        f"[ORDER MANAGER] Network failure submitting exit leg: {e}"
                    )
                    await send_alert(
                        f"CRITICAL: Network failure on exit execution — {e}"
                    )

        logger.success(
            f"[ORDER MANAGER] Exit sequence completed for {position_id} ({exit_reason})."
        )
        return True


# Single instance
order_manager = OrderManager()
