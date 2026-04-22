import math
from loguru import logger
from analytics.atr_engine import atr_engine
from data.runtime_config import runtime_config
from risk.portfolio_state import portfolio_state
from config.constants import SPREAD_WIDTH


class PositionSizer:
    """
    Calculates the number of lots for a trade, perfectly aligned
    with the 'Unchained' backtest logic.
    """

    def __init__(self):
        self._last_margin_state = False

    def calculate(self, spread_type: str = None) -> tuple:
        if not atr_engine.is_ready:
            logger.warning("[POSITION SIZER] ATR engine not ready.")
            return 0, 0.0, 0.0

        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            return 0, 0.0, 0.0

        atr = atr_engine.atr
        atr_multiplier = runtime_config.regime.atr_multiplier
        stop_distance = atr_multiplier * atr

        # 1. Structural Risk Ceiling
        width_pts = SPREAD_WIDTH * nifty.strike_interval
        max_risk_per_spread_rupees = width_pts * nifty.lot_size
        lots_by_risk = int(
            runtime_config.risk.max_risk_per_trade / max_risk_per_spread_rupees
        )

        pe_lots = sum(
            p.lots for p in portfolio_state.open_positions.values() if p.spread_type == "CREDIT_PUT"
        )
        ce_lots = sum(
            p.lots for p in portfolio_state.open_positions.values() if p.spread_type == "CREDIT_CALL"
        )

        # --- IRON CONDOR PAIRING LOGIC (Bypass Margin Double Count) ---
        # If we are submitting a CALL and there's an unmatched PUT, it requires NO new margin
        if spread_type == "CREDIT_CALL" and pe_lots > ce_lots:
            unmatched = pe_lots - ce_lots
            lots = min(unmatched, lots_by_risk)
            return self._calculate_stops(lots, stop_distance, max_risk_per_spread_rupees, nifty.lot_size)
        
        if spread_type == "CREDIT_PUT" and ce_lots > pe_lots:
            unmatched = ce_lots - pe_lots
            lots = min(unmatched, lots_by_risk)
            return self._calculate_stops(lots, stop_distance, max_risk_per_spread_rupees, nifty.lot_size)
        # -------------------------------------------------------------

        from execution.margin_cache import margin_cache
        live_margin = margin_cache.get()

        total_condors_deployed = max(pe_lots, ce_lots)
        deployed = total_condors_deployed * live_margin
        available_capital = portfolio_state.total_capital - deployed
        lots_by_margin = (
            int(available_capital / live_margin) if live_margin > 0 else 0
        )


        # 3. Execution Size (Scale down to fit margin, preventing rejection)
        lots = max(1, min(lots_by_risk, lots_by_margin))

        # Check if even 1 lot violates margin
        if lots_by_margin < 1:
            if not self._last_margin_state:
                logger.warning("[POSITION SIZER] Insufficient margin for even 1 lot.")
                self._last_margin_state = True
            return 0, 0.0, 0.0

        # If we have lots, reset the warning suppression
        self._last_margin_state = False

        # --- NEW: Risk Clamp ---
        # Ensure the calculated stop loss does not exceed the session Circuit Breaker.
        return self._calculate_stops(lots, stop_distance, max_risk_per_spread_rupees, nifty.lot_size)

    def _calculate_stops(self, lots: int, stop_distance: float, max_risk_per_spread_rupees: float, lot_size: int) -> tuple:
        raw_stop_value = stop_distance * lot_size * lots
        max_structural_risk = max_risk_per_spread_rupees * lots
        effective_stop_pts = min(raw_stop_value, max_structural_risk)
        max_allowed_stop = runtime_config.risk.daily_circuit_breaker - 500.0

        if effective_stop_pts > max_allowed_stop:
            logger.warning(
                f"[POSITION SIZER] Capping stop (₹{effective_stop_pts:,.2f}) "
                f"at Circuit Breaker limit (₹{max_allowed_stop:,.2f})"
            )
            stop_loss_value = -max_allowed_stop
        else:
            stop_loss_value = -effective_stop_pts

        logger.info(
            f"[POSITION SIZER] Executing Lots: {lots} | Stop Dist: {stop_distance:.2f} pts | Stop Loss: ₹{stop_loss_value:,.2f}"
        )
        return lots, stop_distance, stop_loss_value


# Single instance
position_sizer = PositionSizer()
