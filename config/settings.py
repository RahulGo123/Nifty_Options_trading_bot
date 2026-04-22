"""
config/settings.py
==================
Global system settings and environment variables.
"""

from dataclasses import dataclass
from decouple import config


@dataclass
class Settings:
    # Execution
    live_trading: bool = config("LIVE_TRADING", cast=bool, default=False)

    # DhanHQ
    dhan_client_id: str = config("DHAN_CLIENT_ID", default="")
    dhan_access_token: str = config("DHAN_ACCESS_TOKEN", default="")
    dhan_pin: str = config("DHAN_PIN", default="")
    dhan_totp_secret: str = config("DHAN_TOTP_SECRET", default="")

    # Database
    database_url: str = config("DATABASE_URL")

    # Telegram
    telegram_token: str = config("TELEGRAM_BOT_TOKEN", default="")
    telegram_chat_id: str = config("TELEGRAM_CHAT_ID", default="")

    # Capital (Matches Dhan ₹1L account reality)
    total_capital: float = config("TOTAL_CAPITAL", cast=float, default=200000.0)


# Single instance used across entire system
settings = Settings()
