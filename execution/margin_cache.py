"""
execution/margin_cache.py
=========================
Zero-latency margin management for the live execution pipeline.
Dynamically handles API fetching, fallback tiers, and SEBI Expiry Escalation.
"""

import json
import requests
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from loguru import logger
from data.runtime_config import runtime_config


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DHAN_MULTI_MARGIN_URL = "https://api.dhan.co/v2/margincalculator/multi"
CACHE_FILE = Path(".margin_cache.json")

# The Progressive Fallback Ladder (Triggered on API Rejection)
# Based on Zerodha SPAN calculator for 1-lot NIFTY Vertical Spread (300-pt wings):
# Real margin = ~₹83,366. Tiers escalate 10% each step.
MARGIN_TIERS = [90_000.0, 1_00_000.0, 1_15_000.0]

NIFTY_SEGMENT = "NSE_FNO"

MARGIN_SAFETY_BUFFER = 1.05  # 5% buffer on top of exact spread margin


class MarginCache:
    def __init__(self):
        self._tier_index: int = 0
        self._margin_per_condor: float = MARGIN_TIERS[self._tier_index]
        self._fetched_at: Optional[datetime] = None
        self._fetch_date: Optional[date] = None
        self._source: str = "fallback_base"

        # SEBI Peak Margin Multiplier (Applied on T-2 and T-1 of Expiry)
        self._escalation_multiplier: float = 1.10

        self._load_from_file()

    # ── Hot Path (Zero I/O) ──────────────────────────────────────────────────

    def get(self) -> float:
        """
        Returns the required margin per Condor/Spread.
        Automatically applies the 1.3x SEBI multiplier if within T-2 days of expiry.
        """
        base_margin = self._margin_per_condor

        if self._is_escalation_active():
            escalated_margin = base_margin * self._escalation_multiplier
            # Only log at debug so we don't spam the console every 1 second during the signal loop
            logger.debug(
                f"[MARGIN CACHE] Expiry escalation ACTIVE. "
                f"Base: ₹{base_margin:,.0f} -> Escalated: ₹{escalated_margin:,.0f}"
            )
            return escalated_margin

        return base_margin

    def max_lots(self, available_capital: float, capital_fraction: float = 0.90) -> int:
        usable = available_capital * capital_fraction
        # Uses self.get() to automatically account for SEBI expiry escalation
        lots = int(usable / self.get())
        return max(1, lots)

    def is_stale(self) -> bool:
        return self._fetch_date != date.today()

    def status(self) -> dict:
        return {
            "tier_index": self._tier_index,
            "margin_per_condor": self._margin_per_condor,
            "source": self._source,
            "fetched_at": self._fetched_at.isoformat() if self._fetched_at else None,
            "fetch_date": self._fetch_date.isoformat() if self._fetch_date else None,
            "is_stale": self.is_stale(),
            "escalation_active": self._is_escalation_active(),
        }

    # ── Escalation Engines ───────────────────────────────────────────────────

    def _is_escalation_active(self) -> bool:
        """
        Determines if we are in the SEBI Peak Margin window (T-2 days from expiry).
        Dynamically adapts to any expiry day (e.g., Tuesday Nifty expiries).
        """
        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty or not nifty.next_expiry:
            return False

        today = date.today()
        days_to_expiry = (nifty.next_expiry - today).days

        # If we are within 2 days of expiry (0 = Expiry Day, 1 = T-1, 2 = T-2)
        if 0 <= days_to_expiry <= 2:
            return True

        return False

    def escalate_margin(self) -> bool:
        """
        Triggered by the execution engine if Dhan rejects an order for Insufficient Margin.
        Bumps the internal margin requirement to the next tier.
        """
        if self._tier_index < len(MARGIN_TIERS) - 1:
            self._tier_index += 1
            self._margin_per_condor = MARGIN_TIERS[self._tier_index]
            self._source = f"escalated_tier_{self._tier_index}"

            logger.warning(
                f"[MARGIN] Broker rejected order. Escalating base margin requirement "
                f"to ₹{self._margin_per_condor:,.0f} per condor."
            )
            self._update_cache(self._margin_per_condor, self._source)
            return True

        logger.critical(
            "[MARGIN] Max margin tier (₹1L) reached. Cannot escalate further."
        )
        return False

    # ── Intraday & Startup Refresh ───────────────────────────────────────────

    def refresh(
        self,
        lot_size: int = 65,
        legs: list = None,
        # Legacy 2-leg args kept for backward compatibility
        proxy_short_token: str = "",
        proxy_long_token: str = "",
    ) -> bool:
        """
        Fetches true SPAN margin from Dhan's multi-margin API.
        Sends the full Iron Condor (4 legs) for an accurate figure.
        Triggered at 09:15 AM and every 60 minutes via market_scheduler.
        """
        logger.info("[MARGIN] Refreshing margin cache from Dhan API...")

        # Build legs list from legacy args if not provided directly
        if not legs:
            if not proxy_short_token or not proxy_long_token:
                logger.warning(
                    "[MARGIN] Missing proxy tokens. Using cached/fallback margin."
                )
                return False
            legs = [
                {"token": proxy_short_token, "action": "SELL"},
                {"token": proxy_long_token, "action": "BUY"},
            ]

        try:
            from auth.dhan_auth import auth_engine

            scrip_list = []
            for leg in legs:
                scrip_list.append(
                    {
                        "exchangeSegment": NIFTY_SEGMENT,
                        "transactionType": "SELL" if leg["action"] == "SELL" else "BUY",
                        "quantity": lot_size,
                        "productType": "MARGIN",
                        "securityId": leg["token"],
                        "price": 0.0,
                    }
                )

            payload = {
                "includePosition": False,
                "includeOrder": False,
                "dhanClientId": auth_engine.client_id,
                "scripList": scrip_list,
            }

            resp = requests.post(
                DHAN_MULTI_MARGIN_URL,
                headers=auth_engine.get_headers(),
                json=payload,
                timeout=8,
            )

            if resp.status_code == 200:
                data = resp.json()
                total_margin = float(
                    data.get("totalMargin") or data.get("total_margin") or 0
                )
                span_margin = float(
                    data.get("spanMargin") or data.get("span_margin") or 0
                )
                exposure = float(
                    data.get("exposure") or data.get("exposure_margin") or 0
                )

                spread_margin = (
                    total_margin if total_margin > 0 else (span_margin + exposure)
                )

                if spread_margin > 0:
                    buffered = spread_margin * MARGIN_SAFETY_BUFFER
                    self._update_cache(buffered, source="dhan_api_multi")
                    logger.info(
                        f"[MARGIN] Iron Condor margin from Dhan: ₹{spread_margin:,.0f} "
                        f"→ ₹{buffered:,.0f} (buffered +5%, cached)"
                    )
                    return True

            logger.warning(
                f"[MARGIN] API failure/zero margin. HTTP {resp.status_code}: {resp.text[:200]}"
            )

        except requests.Timeout:
            logger.warning("[MARGIN] Margin API timed out after 8s.")
        except Exception as e:
            logger.warning(f"[MARGIN] Margin API error: {e}.")

        logger.warning(
            f"[MARGIN] Retaining cache: ₹{self._margin_per_condor:,.0f} ({self._source})"
        )
        return False

    # ── Cache Persistence ────────────────────────────────────────────────────

    def _update_cache(self, margin: float, source: str) -> None:
        self._margin_per_condor = margin
        self._fetched_at = datetime.now(tz=timezone.utc)
        self._fetch_date = date.today()
        self._source = source

        try:
            CACHE_FILE.write_text(
                json.dumps(
                    {
                        "tier_index": self._tier_index,
                        "margin_per_condor": self._margin_per_condor,
                        "fetched_at": self._fetched_at.isoformat(),
                        "fetch_date": self._fetch_date.isoformat(),
                        "source": self._source,
                    }
                ),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"[MARGIN] Could not persist margin cache: {e}")

    def _load_from_file(self) -> None:
        if not CACHE_FILE.exists():
            logger.info(
                f"[MARGIN] No cache file found. Fallback: ₹{MARGIN_TIERS[0]:,.0f}"
            )
            return

        try:
            data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
            margin = float(data.get("margin_per_condor", 0))
            tier_idx = int(data.get("tier_index", 0))
            fetch_date = date.fromisoformat(data.get("fetch_date", "2000-01-01"))
            fetched_at = data.get("fetched_at", "")
            source = data.get("source", "file")

            if margin > 0:
                self._margin_per_condor = margin
                self._tier_index = min(tier_idx, len(MARGIN_TIERS) - 1)
                self._fetch_date = fetch_date
                self._source = f"file ({source})"
                if fetched_at:
                    self._fetched_at = datetime.fromisoformat(fetched_at)

            age = "today" if fetch_date == date.today() else f"from {fetch_date}"
            logger.info(f"[MARGIN] Loaded cached margin: ₹{margin:,.0f} ({age})")

        except Exception as e:
            logger.warning(f"[MARGIN] Failed to load margin cache file: {e}")


margin_cache = MarginCache()
