from datetime import date
from loguru import logger
from monitoring.alerting import send_alert
from config.constants import (
    STT_RATE,
    SEBI_CHARGE_RATE,
    NSE_TRANSACTION_RATE,
    BROKERAGE_PER_LEG,
    GST_RATE,
    DEFAULT_RISK_FREE_RATE,
    RATE_LAST_VERIFIED_DATE,
    RATE_WARN_DAYS,
    RATE_HIGH_DAYS,
    RATE_CRITICAL_DAYS,
    RFR_LAST_UPDATED_DATE,
    RFR_STALE_DAYS,
)


class RateValidator:
    """
    Runs at 08:45 AM every morning during pre-market init.

    Two separate staleness checks:

    1. Transaction cost rates (STT, SEBI, NSE, brokerage, GST)
       These change via PDF circulars — no public API exists.
       Manual verification required.
       Alert fires at 180 / 270 / 365 days since last check.

    2. Risk-free rate (RBI 91-day T-bill yield)
       Changes weekly after every Tuesday RBI auction.
       Tracked separately from transaction costs because
       update frequency is completely different.
       Alert fires after 10 days without update.

    Neither check is fatal — the system always continues.
    But the alerts are loud enough that you cannot ignore them.

    How to reset alerts:
    - After verifying transaction costs:
        Update RATE_LAST_VERIFIED_DATE in constants.py
    - After updating risk-free rate:
        Update DEFAULT_RISK_FREE_RATE in constants.py
        Update RFR_LAST_UPDATED_DATE in constants.py
    - Redeploy after either change
    """

    def __init__(self):
        pass

    async def run(self) -> bool:
        """
        Master method called at 08:45 AM.
        Always returns True — rate staleness never halts the system.
        """
        logger.info("[RATE VALIDATOR] Starting rate validation...")

        await self._check_transaction_rate_staleness()
        await self._check_risk_free_rate_staleness()

        logger.info("[RATE VALIDATOR] Rate validation complete.")
        return True

    async def _check_transaction_rate_staleness(self) -> None:
        """
        Checks how long ago transaction cost rates were
        manually verified against NSE/SEBI circulars.
        Fires escalating Telegram alerts.
        """
        today = date.today()
        days_since = (today - RATE_LAST_VERIFIED_DATE).days

        rate_summary = (
            f"Current rates — "
            f"STT: {STT_RATE*100:.4f}% | "
            f"SEBI: {SEBI_CHARGE_RATE*100:.6f}% | "
            f"NSE: {NSE_TRANSACTION_RATE*100:.6f}% | "
            f"Brokerage: ₹{BROKERAGE_PER_LEG}/leg | "
            f"GST: {GST_RATE*100:.0f}%"
        )

        verification_urls = (
            "Verify at: "
            "nseindia.com/regulations/circulars | "
            "sebi.gov.in/legal/circulars"
        )

        if days_since >= RATE_CRITICAL_DAYS:
            msg = (
                f"CRITICAL: Transaction cost rates NOT verified "
                f"in {days_since} days "
                f"(last: {RATE_LAST_VERIFIED_DATE}). "
                f"P&L calculations may be materially wrong. "
                f"{rate_summary}. {verification_urls}"
            )
            logger.error(f"[RATE VALIDATOR] {msg}")
            await send_alert(msg)

        elif days_since >= RATE_HIGH_DAYS:
            msg = (
                f"HIGH: Transaction cost rates last verified "
                f"{days_since} days ago ({RATE_LAST_VERIFIED_DATE}). "
                f"Verify against latest NSE/SEBI circulars. "
                f"{rate_summary}"
            )
            logger.warning(f"[RATE VALIDATOR] {msg}")
            await send_alert(msg)

        elif days_since >= RATE_WARN_DAYS:
            msg = (
                f"WARNING: Transaction cost rates last verified "
                f"{days_since} days ago ({RATE_LAST_VERIFIED_DATE}). "
                f"Consider verifying against NSE/SEBI circulars. "
                f"{rate_summary}"
            )
            logger.warning(f"[RATE VALIDATOR] {msg}")
            await send_alert(msg)

        else:
            logger.info(
                f"[RATE VALIDATOR] Transaction cost rates current. "
                f"Last verified: {RATE_LAST_VERIFIED_DATE} "
                f"({days_since} days ago). "
                f"{rate_summary}"
            )

    async def _check_risk_free_rate_staleness(self) -> None:
        """
        Checks whether the risk-free rate has been updated
        since the last RBI T-bill auction.

        RBI holds 91-day T-bill auctions every Tuesday.
        Alert fires if more than RFR_STALE_DAYS have passed
        since RFR_LAST_UPDATED_DATE.

        To update:
        1. Visit rbi.org.in after each Tuesday auction
        2. Find 91-day T-bill cut-off yield
        3. Update DEFAULT_RISK_FREE_RATE in constants.py
        4. Update RFR_LAST_UPDATED_DATE in constants.py
        5. Redeploy
        """
        today = date.today()
        days_since = (today - RFR_LAST_UPDATED_DATE).days

        if days_since >= RFR_STALE_DAYS:
            msg = (
                f"WARNING: Risk-free rate (currently "
                f"{DEFAULT_RISK_FREE_RATE*100:.4f}%) "
                f"last updated {days_since} days ago "
                f"({RFR_LAST_UPDATED_DATE}). "
                f"Check RBI 91-day T-bill yield at rbi.org.in "
                f"and update DEFAULT_RISK_FREE_RATE and "
                f"RFR_LAST_UPDATED_DATE in constants.py if changed."
            )
            logger.warning(f"[RATE VALIDATOR] {msg}")
            await send_alert(msg)
        else:
            logger.info(
                f"[RATE VALIDATOR] Risk-free rate current: "
                f"{DEFAULT_RISK_FREE_RATE*100:.4f}% "
                f"(updated {days_since} days ago)."
            )


# Single instance
rate_validator = RateValidator()
