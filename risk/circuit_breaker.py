"""
risk/circuit_breaker.py
=======================
Institutional Daily Loss Limit Enforcer.
"""

from loguru import logger
from data.runtime_config import runtime_config
from risk.portfolio_state import portfolio_state
from monitoring.alerting import send_alert


class CircuitBreaker:
    """
    Monitors daily P&L and enforces the hard drawdown limit.
    Uses runtime_config.risk.daily_circuit_breaker for dynamic limits.
    """

    async def evaluate(self) -> tuple[bool, str]:
        """
        Checks whether new positions are permitted.
        Returns (True, "OK") if trading is allowed.
        Returns (False, Reason) if the circuit breaker is tripped.
        """
        # Already tripped — no new entries
        if portfolio_state.circuit_breaker_tripped:
            return False, "CIRCUIT_BREAKER_ALREADY_TRIPPED"

        if runtime_config.risk is None:
            logger.error("[CIRCUIT BREAKER] Risk config not loaded.")
            return False, "RISK_CONFIG_MISSING"

        daily_pnl = portfolio_state.daily_pnl
        # limit is defined as a positive number in config (e.g., 50000), so we negate it
        limit = -runtime_config.risk.daily_circuit_breaker

        if daily_pnl <= limit:
            # Trip the circuit breaker
            portfolio_state.trip_circuit_breaker()

            msg = (
                f"🛑 <b>CIRCUIT BREAKER TRIPPED</b>\n"
                f"<b>Daily P&L:</b> 🔴 -₹{abs(daily_pnl):,.2f}\n"
                f"<b>Limit:</b> -₹{abs(limit):,.2f}\n"
                f"<b>Action:</b> All new entries HALTED for the session.\n"
                f"<b>Status:</b> Existing positions will be managed/squared off."
            )
            logger.critical(
                f"[CIRCUIT BREAKER] LIMIT EXCEEDED. P&L: ₹{daily_pnl:,.2f} / Limit: ₹{limit:,.2f}"
            )
            await send_alert(msg)
            return False, "CIRCUIT_BREAKER_JUST_TRIPPED"

        # Log a warning when approaching the limit (75% of limit)
        warning_threshold = limit * 0.75
        if daily_pnl <= warning_threshold:
            logger.warning(
                f"[CIRCUIT BREAKER] Approaching limit. "
                f"Daily P&L: ₹{daily_pnl:,.2f} | "
                f"Limit: ₹{limit:,.2f} | "
                f"Remaining buffer: ₹{daily_pnl - limit:,.2f}"
            )

        return True, "OK"

    @property
    def is_tripped(self) -> bool:
        """True if the circuit breaker has been tripped."""
        return portfolio_state.circuit_breaker_tripped


# Single instance
circuit_breaker = CircuitBreaker()
