"""
auth/dhan_auth.py
=================
DhanHQ Authentication Engine — Updated for 24-hour token policy.

Token lifecycle (per SEBI guidelines):
  - Trading API token:  valid for 24 hours only
  - Data API token:     valid for 1 month (monthly) or 1 year (yearly)

Two refresh strategies (attempted in order):
  1. RenewToken  — POST /v2/RenewToken
                   Extends the current token for another 24 hours.
                   Works only when the current token is still ACTIVE.
                   Requires no credentials — just the existing token.

  2. TOTP Login  — POST /auth/app/generateAccessToken?clientId=...&pin=...&totp=...
                   Generates a brand-new token from scratch.
                   Requires DHAN_PIN + DHAN_TOTP_SECRET in .env.
                   Use this as fallback when the current token has already expired.

.env keys required:
  DHAN_CLIENT_ID      — Your Dhan client ID (numeric string)
  DHAN_ACCESS_TOKEN   — Current access token (updated automatically on refresh)
  DHAN_PIN            — Your Dhan login PIN (for TOTP fallback)
  DHAN_TOTP_SECRET    — TOTP secret key from Dhan (for TOTP fallback)
                        Enable TOTP at: web.dhan.co → Profile → Security

Token file:
  Token is persisted to .dhan_token (in project root) after every refresh,
  so restarts pick up the latest token automatically without re-authenticating.
"""

import json
import pyotp
import requests
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger
from decouple import config

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DHAN_API_BASE = "https://api.dhan.co/v2"
DHAN_AUTH_BASE = "https://auth.dhan.co/app"
RENEW_TOKEN_URL = f"{DHAN_API_BASE}/RenewToken"
GEN_TOKEN_URL = f"{DHAN_AUTH_BASE}/generateAccessToken"
USER_PROFILE_URL = (
    f"{DHAN_API_BASE}/fundlimit"  # lightweight endpoint to probe token health
)

TOKEN_FILE = Path(".dhan_token")  # persisted token cache
TOKEN_BUFFER_SECS = 300  # refresh 5 min before expiry


# ---------------------------------------------------------------------------
# DhanAuthEngine
# ---------------------------------------------------------------------------


class DhanAuthEngine:
    """
    Manages Dhan API authentication with automatic 24-hour token refresh.

    Usage:
        auth = DhanAuthEngine()
        headers = auth.get_headers()   # always returns a valid header dict
    """

    def __init__(self):
        self.client_id: str = config("DHAN_CLIENT_ID", default="")
        self._access_token: str = ""
        self._expiry: datetime = datetime.min.replace(tzinfo=timezone.utc)

        # TOTP fallback credentials (optional — set in .env to enable)
        self._pin: str = config("DHAN_PIN", default="")
        self._totp_secret: str = config("DHAN_TOTP_SECRET", default="")

        # Load the persisted token first, then try to refresh it
        self._load_persisted_token()
        self.ensure_valid_token()

    # ── Public interface ──────────────────────────────────────────────────────

    # ── Public Properties ─────────────────────────────────────────────────────
    @property
    def access_token(self) -> str:
        """Exposes the access token safely for the WebSocket manager."""
        self.ensure_valid_token()
        return self._access_token

    def get_headers(self) -> dict:
        """
        Returns authenticated headers for every Dhan REST API call.
        Automatically refreshes the token if it is about to expire.
        """
        self.ensure_valid_token()

        if not self.client_id or not self._access_token:
            raise RuntimeError(
                "[AUTH] DHAN_CLIENT_ID or access token missing. "
                "Check .env and .dhan_token file."
            )

        return {
            "access-token": self._access_token,
            "client-id": self.client_id,
            "dhanClientId": self.client_id,  # some v2 endpoints use this
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def ensure_valid_token(self) -> None:
        """
        Checks if the current token is still valid (with buffer).
        If not, attempts refresh via RenewToken → TOTP fallback.
        Called automatically by get_headers() on every request.
        """
        now = datetime.now(tz=timezone.utc)
        seconds_left = (self._expiry - now).total_seconds()

        if seconds_left > TOKEN_BUFFER_SECS:
            return  # token still healthy

        if seconds_left <= 0:
            logger.warning(
                f"[AUTH] Token expired at {self._expiry.isoformat()}. "
                "Attempting fresh generation via TOTP."
            )
        else:
            logger.info(
                f"[AUTH] Token expires in {int(seconds_left)}s — refreshing now."
            )

        # Strategy 1: RenewToken (token still active but near expiry)
        if seconds_left > 0 and self._access_token:
            if self._renew_token():
                return

        # Strategy 2: TOTP-based generation (token already expired)
        if self._pin and self._totp_secret:
            if self._generate_token_totp():
                return

        # Both failed — log but don't crash; existing token may still work briefly
        logger.error(
            "[AUTH] All refresh strategies failed. "
            "Bot will continue with current token — it may fail on next API call."
        )

    def is_authenticated(self) -> bool:
        """Returns True if a non-empty token exists."""
        return bool(self.client_id and self._access_token)

    # ── Refresh Strategy 1: RenewToken ───────────────────────────────────────

    def _renew_token(self) -> bool:
        """
        Calls POST /v2/RenewToken to extend the current token by 24 hours.
        Only works when the token is still active (not yet expired).
        Returns True on success.
        """
        try:
            resp = requests.post(
                RENEW_TOKEN_URL,
                headers={
                    "access-token": self._access_token,
                    "dhanClientId": self.client_id,
                    "Content-Type": "application/json",
                },
                timeout=10,
            )

            if resp.status_code == 200:
                data = resp.json()
                new_token = data.get("accessToken") or data.get("access_token")
                expiry_str = data.get("expiryTime") or data.get("expiry_time")

                if new_token:
                    self._update_token(new_token, expiry_str)
                    logger.info(
                        f"[AUTH] Token renewed via RenewToken. "
                        f"Expires: {self._expiry.isoformat()}"
                    )
                    return True

            logger.warning(
                f"[AUTH] RenewToken failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )

        except Exception as e:
            logger.warning(f"[AUTH] RenewToken error: {e}")

        return False

    # ── Refresh Strategy 2: TOTP-based generation ────────────────────────────

    def _generate_token_totp(self) -> bool:
        """
        Generates a fresh 24-hour token using PIN + TOTP.
        Requires DHAN_PIN and DHAN_TOTP_SECRET in .env.
        Returns True on success.
        """
        try:
            # 1. Aggressively strip whitespace to prevent silent Dhan validation errors
            clean_client = self.client_id.strip()
            clean_pin = self._pin.strip()
            clean_secret = self._totp_secret.strip()

            totp = pyotp.TOTP(clean_secret).now()

            # 2. URL Parameters (Strict Dhan Requirement for this endpoint)
            url = f"{GEN_TOKEN_URL}?dhanClientId={clean_client}&pin={clean_pin}&totp={totp}"

            # 3. Bare-minimum browser spoof headers
            headers = {
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Content-Length": "0",
            }

            resp = requests.post(url, headers=headers, timeout=10)

            if resp.status_code == 200:
                data = resp.json()

                # Catch Dhan's generic HTTP 200 OK errors
                if data.get("status") == "error":
                    logger.error(
                        f"[AUTH] Dhan rejected credentials: {data.get('message')}"
                    )
                    return False

                new_token = data.get("accessToken") or data.get("access_token")
                expiry_str = data.get("expiryTime") or data.get("expiry_time")

                if new_token:
                    self._update_token(new_token, expiry_str)
                    logger.info(
                        f"[AUTH] Fresh token generated via TOTP. "
                        f"Expires: {self._expiry.isoformat()}"
                    )
                    return True

            logger.error(
                f"[AUTH] TOTP token generation failed: "
                f"HTTP {resp.status_code} — {resp.text[:200]}"
            )

        except Exception as e:
            logger.error(f"[AUTH] TOTP generation error: {e}")

        return False

    # ── Token persistence ─────────────────────────────────────────────────────

    def _update_token(self, new_token: str, expiry_str: str | None) -> None:
        """
        Updates the in-memory token and expiry, then persists to .dhan_token.
        """
        self._access_token = new_token

        # Parse expiry — Dhan returns ISO 8601 string, e.g. "2026-01-01T00:00:00.000"
        if expiry_str:
            try:
                # Strip milliseconds if present, ensure UTC
                clean = expiry_str.split(".")[0]
                self._expiry = datetime.fromisoformat(clean).replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                # Fallback: assume 24 hours from now
                from datetime import timedelta

                self._expiry = datetime.now(tz=timezone.utc) + timedelta(hours=24)
        else:
            from datetime import timedelta

            self._expiry = datetime.now(tz=timezone.utc) + timedelta(hours=24)

        # Persist to file so the next process startup picks it up
        try:
            TOKEN_FILE.write_text(
                json.dumps(
                    {
                        "access_token": self._access_token,
                        "expiry": self._expiry.isoformat(),
                        "client_id": self.client_id,
                        "refreshed_at": datetime.now(tz=timezone.utc).isoformat(),
                    }
                ),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"[AUTH] Could not persist token to {TOKEN_FILE}: {e}")

    def _load_persisted_token(self) -> None:
        """
        Loads a previously persisted token from .dhan_token.
        Falls back to .env DHAN_ACCESS_TOKEN if the file doesn't exist.
        """
        # Try .dhan_token file first (most recently refreshed token)
        if TOKEN_FILE.exists():
            try:
                data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
                token = data.get("access_token", "")
                expiry_str = data.get("expiry", "")

                if token:
                    self._access_token = token
                    if expiry_str:
                        self._expiry = datetime.fromisoformat(expiry_str)
                    logger.info(
                        f"[AUTH] Loaded persisted token from {TOKEN_FILE}. "
                        f"Expires: {self._expiry.isoformat()}"
                    )
                    return
            except Exception as e:
                logger.warning(f"[AUTH] Failed to load {TOKEN_FILE}: {e}")

        # Fallback to .env value
        env_token = config("DHAN_ACCESS_TOKEN", default="")
        if env_token:
            self._access_token = env_token
            # Assume worst case: could expire any time, will be checked on first use
            from datetime import timedelta

            self._expiry = datetime.now(tz=timezone.utc) + timedelta(hours=1)
            logger.info("[AUTH] Loaded token from .env (expiry unknown — will probe).")


# ---------------------------------------------------------------------------
# Single instance used across the entire trading system
# ---------------------------------------------------------------------------
auth_engine = DhanAuthEngine()
