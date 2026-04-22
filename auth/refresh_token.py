import os
import sys
import pyotp
import dotenv
import requests
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)


def refresh_dhan_token():
    try:
        env_path = ".env"
        dotenv.load_dotenv(env_path)

        # THE FIX: .strip() removes all invisible spaces, tabs, and newline characters
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        pin = os.getenv("DHAN_PIN", "").strip()
        totp_secret = os.getenv("DHAN_TOTP_SECRET", "").strip()

        if not all([client_id, pin, totp_secret]):
            raise ValueError(
                "FATAL: Missing DHAN_CLIENT_ID, DHAN_PIN, or DHAN_TOTP_SECRET in .env."
            )

        # 1. Generate the TOTP
        totp_code = pyotp.TOTP(totp_secret).now()

        logging.info(f"Loaded Client ID: {client_id}")
        logging.info(f"Generated TOTP: {totp_code}")

        # 2. Revert to URL parameters (Dhan's exact requirement for this endpoint)
        url = f"https://auth.dhan.co/app/generateAccessToken?dhanClientId={client_id}&pin={pin}&totp={totp_code}"

        # 3. Bare-minimum headers to prevent WAF rejection on empty POST requests
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Content-Length": "0",
        }

        logging.info("Firing POST request to Dhan auth server...")
        response = requests.post(url, headers=headers, timeout=10)

        if response.status_code != 200:
            raise RuntimeError(
                f"API request failed with status {response.status_code}: {response.text}"
            )

        access_token_data = response.json()

        # Catch Dhan's generic 200 OK errors
        if access_token_data.get("status") == "error":
            raise RuntimeError(
                f"Dhan rejected credentials: {access_token_data.get('message')}"
            )

        new_token = access_token_data.get(
            "accessToken", access_token_data.get("access_token")
        )

        if not new_token:
            raise RuntimeError(
                f"Token missing from response. Data: {access_token_data}"
            )

        # Overwrite .env securely
        dotenv.set_key(env_path, "DHAN_ACCESS_TOKEN", new_token)

        # Also write .dhan_token in the format dhan_auth.py expects
        import json
        from datetime import datetime, timezone, timedelta
        from pathlib import Path

        expiry = datetime.now(tz=timezone.utc) + timedelta(hours=24)
        token_data = {
            "access_token": new_token,
            "expiry": expiry.isoformat(),
            "client_id": client_id,
            "refreshed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        Path(".dhan_token").write_text(json.dumps(token_data), encoding="utf-8")

        logging.info(
            "Success! Pipeline is fully authenticated. New token saved to .env and .dhan_token."
        )

    except Exception as e:
        logging.error(f"Authentication Engine Crashed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    refresh_dhan_token()
