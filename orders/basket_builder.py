from loguru import logger
from data.runtime_config import runtime_config
from config.constants import SPREAD_WIDTH
from monitoring.alerting import send_alert
import asyncio


class BasketBuilder:
    """
    Constructs spread order payloads matching the backtest exactly.

    Iron Condor Structure (from backtest_engine.py):
        CREDIT_PUT:  Short = ATM - 1×interval, Long = ATM - 1×interval - width
        CREDIT_CALL: Short = ATM + 1×interval, Long = ATM + 1×interval + width

    width = SPREAD_WIDTH × strike_interval  (e.g. 6 × 50 = 300 pts)
    """

    def __init__(self):
        self._token_alert_sent: bool = False

    def build(
        self,
        spread_type: str,
        atm_strike: int,
        lots: int,
    ) -> dict:
        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            logger.error("[BASKET BUILDER] NIFTY instrument config not found.")
            return None

        if not nifty.instrument_tokens:
            logger.error("[BASKET BUILDER] Token map is empty.")
            return None

        interval = nifty.strike_interval
        width_pts = SPREAD_WIDTH * interval  # 6 × 50 = 300 pts

        # --- Match backtest _open_position exactly ---
        if spread_type == "CREDIT_PUT":
            option_type = "PE"
            short_strike = atm_strike - interval          # ATM - 50
            long_strike  = short_strike - width_pts       # ATM - 350
        elif spread_type == "CREDIT_CALL":
            option_type = "CE"
            short_strike = atm_strike + interval          # ATM + 50
            long_strike  = short_strike + width_pts       # ATM + 350
        else:
            logger.error(f"[BASKET BUILDER] Unknown spread type: {spread_type}")
            return None

        long_token  = self._get_token(long_strike,  option_type, nifty)
        short_token = self._get_token(short_strike, option_type, nifty)

        if long_token is None or short_token is None:
            logger.error(
                f"[BASKET BUILDER] Token not found. "
                f"Long: {long_strike}{option_type} → {long_token} | "
                f"Short: {short_strike}{option_type} → {short_token}"
            )
            if not self._token_alert_sent:
                self._token_alert_sent = True
                asyncio.create_task(send_alert(
                    f"ERROR: Strike out of token map. {long_strike}{option_type} or "
                    f"{short_strike}{option_type} not loaded. Restart bot to reload scrip master."
                ))
            return None

        basket = {
            "symbol": "NIFTY",
            "spread_type": spread_type,
            "expiry": nifty.next_expiry,
            "lots": lots,
            "lot_size": nifty.lot_size,
            "legs": [
                {
                    "leg": "LONG",
                    "strike": long_strike,
                    "option_type": option_type,
                    "token": long_token,
                    "action": "BUY",
                    "lots": lots,
                    "order_type": "MARKET",
                    "product": "MIS",
                },
                {
                    "leg": "SHORT",
                    "strike": short_strike,
                    "option_type": option_type,
                    "token": short_token,
                    "action": "SELL",
                    "lots": lots,
                    "order_type": "MARKET",
                    "product": "MIS",
                },
            ],
        }

        logger.info(
            f"[BASKET BUILDER] {spread_type} | "
            f"Short: {short_strike}{option_type} | Long: {long_strike}{option_type} | "
            f"Lots: {lots}"
        )
        return basket

    def _get_token(self, strike: int, option_type: str, nifty) -> int:
        pair = nifty.instrument_tokens.get(strike)
        if not pair:
            logger.error(f"[BASKET BUILDER] Strike {strike} not in token map.")
            return None
        token = pair.get(option_type)
        if not token:
            logger.error(f"[BASKET BUILDER] {option_type} token not found for strike {strike}.")
            return None
        return token


# Single instance
basket_builder = BasketBuilder()
