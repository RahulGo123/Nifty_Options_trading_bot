"""
risk/reconciliation.py
======================
Institutional Position Reconciliation Engine.
Cross-references internal memory with live broker reality to prevent naked risk.
"""

import aiohttp
from decouple import config
from loguru import logger
from auth.dhan_auth import auth_engine
from risk.portfolio_state import portfolio_state
from monitoring.alerting import send_alert


class ReconciliationEngine:
    def __init__(self):
        self.dhan_positions_url = "https://api.dhan.co/v2/positions"
        self._live_trading = config("LIVE_TRADING", default=False, cast=bool)

    async def run_reconciliation(self) -> bool:
        """
        Fetches live open positions from Dhan and compares them to internal state.
        If a discrepancy is found, it trips the circuit breaker and alerts.
        """
        if not self._live_trading:
            logger.debug("[RECONCILIATION] Ghost mode active. Skipping broker sync.")
            return True

        logger.debug("[RECONCILIATION] Running 5-minute broker sync...")

        if not auth_engine.is_authenticated():
            logger.warning("[RECONCILIATION] Auth engine offline. Skipping sync.")
            return False

        # 1. Fetch the absolute truth from the broker
        broker_positions = await self._fetch_broker_positions()
        if broker_positions is None:
            return False  # API failed, skip this cycle rather than false-alarming

        # 2. Calculate Gross Open Quantities
        broker_gross_qty = self._calculate_broker_gross_qty(broker_positions)
        internal_gross_qty = self._calculate_internal_gross_qty()

        # 3. The Audit
        if broker_gross_qty != internal_gross_qty:
            error_msg = (
                f"🚨 <b>CRITICAL: POSITION DESYNC DETECTED!</b>\n"
                f"Broker Reality: {broker_gross_qty} open lots/shares\n"
                f"Internal Memory: {internal_gross_qty} open lots/shares\n\n"
                f"<b>Action:</b> Tripping Circuit Breaker. Manual intervention required immediately."
            )
            logger.critical(
                f"[RECONCILIATION] DESYNC! Broker: {broker_gross_qty} | Internal: {internal_gross_qty}"
            )

            # Trip the master circuit breaker to halt all new trading
            portfolio_state.trip_circuit_breaker()
            await send_alert(error_msg)

            return False

        logger.debug("[RECONCILIATION] Audit passed. Memory matches Broker reality.")
        return True

    async def _fetch_broker_positions(self) -> list:
        """Calls the Dhan API to get today's positions."""
        headers = auth_engine.get_headers()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.dhan_positions_url, headers=headers
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Dhan returns a list of positions under 'data'
                        return data if isinstance(data, list) else data.get("data", [])
                    else:
                        logger.warning(
                            f"[RECONCILIATION] Dhan API Error {response.status}: {await response.text()}"
                        )
                        return None
        except Exception as e:
            logger.error(f"[RECONCILIATION] Network error fetching positions: {e}")
            return None

    def _calculate_broker_gross_qty(self, broker_positions: list) -> int:
        """
        Sums up the absolute open quantity across all broker positions.
        (e.g., Short 50 lots = 50. Long 50 lots = 50. Gross = 100).
        """
        gross_qty = 0
        for pos in broker_positions:
            # Depending on Dhan's exact response structure, usually 'netQty' or 'buyQty' - 'sellQty'
            # We only care if the position is currently OPEN (net quantity != 0)
            net_qty = int(pos.get("netQty", 0))
            if pos.get("positionType") == "OPEN" or net_qty != 0:
                gross_qty += abs(net_qty)
        return gross_qty

    def _calculate_internal_gross_qty(self) -> int:
        """
        Sums up the absolute open quantity from our internal portfolio_state.
        """
        gross_qty = 0
        for pos_id, pos in portfolio_state.open_positions.items():
            # A standard spread has two legs (long and short)
            # Lots * Lot Size * 2 legs
            gross_qty += pos.lots * pos.lot_size * 2
        return gross_qty


# Single instance
reconciliation_engine = ReconciliationEngine()
