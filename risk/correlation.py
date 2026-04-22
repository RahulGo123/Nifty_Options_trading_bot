"""
risk/correlation.py
===================
Institutional Correlation Guard.
Prevents the bot from stacking heavily correlated directional risk.
"""

from loguru import logger
from risk.portfolio_state import portfolio_state


class CorrelationGuard:
    def __init__(self):
        # We define how many spreads of the SAME direction we tolerate.
        # 1 means if we have a bearish spread open, we cannot open another one.
        self.max_directional_overlap = 1

    def evaluate(self, proposed_basket: dict) -> tuple[bool, str]:
        """
        Calculates the directional bias of the new basket and compares it
        against the active portfolio to prevent concentrated risk.
        """
        if portfolio_state.position_count == 0:
            return True, "PORTFOLIO_EMPTY_NO_CORRELATION"

        proposed_bias = self._calculate_basket_bias(proposed_basket)

        # If the proposed basket is perfectly neutral (e.g., symmetric Iron Condor),
        # it poses no directional correlation risk.
        if proposed_bias == "NEUTRAL":
            return True, "PROPOSED_BASKET_NEUTRAL"

        current_directional_count = 0

        for pos_id, pos in portfolio_state.open_positions.items():
            # Mocking the position bias check. In your actual code, you will
            # determine if the open 'pos' is BULLISH, BEARISH, or NEUTRAL
            # based on its legs (e.g., Short CE = BEARISH).
            existing_bias = self._get_position_bias(pos)

            if existing_bias == proposed_bias:
                current_directional_count += 1

        if current_directional_count >= self.max_directional_overlap:
            logger.debug(
                f"[CORRELATION GUARD] Evaluation Result: Blocked duplication of {proposed_bias} risk. "
                f"Count: {current_directional_count} | Limit: {self.max_directional_overlap}"
            )
            return False, f"CORRELATION_LIMIT_{proposed_bias}"

        return True, "CORRELATION_CHECK_PASSED"

    def _calculate_basket_bias(self, basket: dict) -> str:
        """
        Determines the directional risk of a basket based on its short legs.
        Short CE -> Bearish. Short PE -> Bullish. Both -> Neutral.
        """
        has_short_ce = False
        has_short_pe = False

        for leg in basket.get("legs", []):
            if leg.get("action") == "SELL":
                if leg.get("option_type") == "CE":
                    has_short_ce = True
                elif leg.get("option_type") == "PE":
                    has_short_pe = True

        if has_short_ce and has_short_pe:
            return "NEUTRAL"  # Iron Condor / Short Straddle
        elif has_short_ce:
            return "BEARISH"  # Call Credit Spread
        elif has_short_pe:
            return "BULLISH"  # Put Credit Spread

        return "UNKNOWN"

    def _get_position_bias(self, pos) -> str:
        """
        Helper to extract the bias from an already open portfolio_state position.
        Matches the logic of _calculate_basket_bias.
        """
        # Adapt this strictly to how your open_positions store leg data.
        # Assuming pos.spread_type holds this data for now.
        spread_type = getattr(pos, "spread_type", "").upper()

        if "CONDOR" in spread_type or "STRANGLE" in spread_type:
            return "NEUTRAL"
        elif "CALL" in spread_type or "CE" in spread_type:
            return "BEARISH"
        elif "PUT" in spread_type or "PE" in spread_type:
            return "BULLISH"

        return "UNKNOWN"


# Single instance
correlation_guard = CorrelationGuard()
