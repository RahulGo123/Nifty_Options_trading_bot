"""
monitoring/alerting.py
======================
Full Telegram alerting implementation.

All alerts are fire-and-forget from the caller's perspective.
This module owns the asyncio queue internally and never blocks
the main event loop on network I/O.

Alert levels
------------
CRITICAL  — circuit breaker, WebSocket dead, auth failure, IV bootstrap failure
WARNING   — basket rejected, scrip master parse error, expiry weekday changed,
            margin pre-check failed, rate staleness warnings
INFO      — trade entries, trade exits, position lifecycle events
EOD       — end-of-day summary report (dispatched by reporter.py)

Configuration (all via .env / python-decouple)
-----------------------------------------------
TELEGRAM_BOT_TOKEN   — bot token from @BotFather
TELEGRAM_CHAT_ID     — target chat or group ID (negative for groups)
TELEGRAM_ENABLED     — True/False master switch (default True)
LIVE_TRADING         — used to prefix messages [LIVE] vs [GHOST]
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum
from typing import Optional

import aiohttp
from decouple import config
from loguru import logger

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOT_TOKEN: str = config("TELEGRAM_BOT_TOKEN", default="")
CHAT_ID: str = config("TELEGRAM_CHAT_ID", default="")
TELEGRAM_ENABLED: bool = config("TELEGRAM_ENABLED", default=True, cast=bool)
LIVE_TRADING: bool = config("LIVE_TRADING", default=False, cast=bool)

_MODE_TAG = "[🔴 LIVE]" if LIVE_TRADING else "[👻 GHOST]"
_TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Maximum characters Telegram allows per message.
_MAX_MSG_LEN = 4096

# How many seconds to wait between retries on network failure.
_RETRY_DELAYS = (2, 5, 10)

# Internal queue for fire-and-forget dispatch.
# Unbounded — messages accumulate during Telegram downtime and drain on recovery.
_queue: asyncio.Queue[str] = asyncio.Queue()

# Background task handle — stored so it is not garbage-collected.
_dispatch_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]


# ---------------------------------------------------------------------------
# Alert level enum (for structured callers)
# ---------------------------------------------------------------------------


class AlertLevel(Enum):
    CRITICAL = "🚨 CRITICAL"
    WARNING = "⚠️  WARNING"
    INFO = "ℹ️  INFO"
    EOD = "📊 EOD REPORT"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _timestamp() -> str:
    """HH:MM:SS IST label — purely cosmetic, no tz dependency."""
    t = time.localtime()
    return f"{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}"


def _truncate(text: str, max_len: int = _MAX_MSG_LEN) -> str:
    """Hard-truncate a message that exceeds Telegram's limit."""
    if len(text) <= max_len:
        return text
    suffix = "\n… [truncated]"
    return text[: max_len - len(suffix)] + suffix


def _build_message(level: AlertLevel, body: str) -> str:
    return _truncate(f"{_MODE_TAG} {level.value}\n" f"🕐 {_timestamp()}\n\n" f"{body}")


async def _send_now(text: str) -> bool:
    """
    Single attempt to POST to Telegram sendMessage.
    Returns True on success, False on any failure.
    Does NOT raise — all exceptions are swallowed and logged.
    """
    if not TELEGRAM_ENABLED:
        logger.info("Telegram disabled. Message suppressed:\n%s", text)
        return True

    if not BOT_TOKEN or not CHAT_ID:
        logger.error(
            "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured. "
            "Message dropped:\n%s",
            text,
        )
        return False

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{_TELEGRAM_API}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    return True
                logger.warning(
                    "Telegram API returned HTTP %s: %s",
                    resp.status,
                    await resp.text(),
                )
                return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("Telegram send failed: %s", exc)
        return False


async def _send_with_retry(text: str) -> None:
    """
    Attempt send with exponential-ish backoff.
    Drops the message after all retries are exhausted — never blocks forever.
    """
    for attempt, delay in enumerate([0] + list(_RETRY_DELAYS), start=1):
        if delay:
            await asyncio.sleep(delay)
        if await _send_now(text):
            return
        logger.warning("Telegram send attempt %d failed.", attempt)
    logger.error(
        "Telegram message permanently dropped after %d attempts.",
        len(_RETRY_DELAYS) + 1,
    )


async def _dispatcher() -> None:
    """
    Background coroutine that drains _queue.
    Runs for the lifetime of the process.
    """
    logger.info("Telegram dispatcher started.")
    while True:
        text = await _queue.get()
        await _send_with_retry(text)
        _queue.task_done()


# ---------------------------------------------------------------------------
# Public API — initialisation
# ---------------------------------------------------------------------------


def start_alerter(loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """
    Must be called once after the asyncio event loop is running.
    Typically called from main.py during Layer 1 initialisation.
    """
    global _dispatch_task
    if _dispatch_task is not None:
        logger.warning("Alerter already started — ignoring duplicate call.")
        return
    _dispatch_task = asyncio.ensure_future(_dispatcher())
    logger.info(
        "Telegram alerter initialised. Enabled=%s Mode=%s", TELEGRAM_ENABLED, _MODE_TAG
    )


async def stop_alerter() -> None:
    """
    Drains the Telegram message queue before shutting down.
    Ensures final reports/alerts are sent.
    """
    global _dispatch_task
    if _dispatch_task is None:
        return

    logger.info("Draining Telegram alert queue...")
    # Wait for all current messages in the queue to be processed
    try:
        if not _queue.empty():
            # Wait for at most 10 seconds for the queue to drain
            await asyncio.wait_for(_queue.join(), timeout=10.0)
            logger.info("Telegram alert queue drained.")
    except asyncio.TimeoutError:
        logger.warning("Telegram alert queue drain timed out. Some alerts may be lost.")
    except Exception as e:
        logger.error(f"Error draining alert queue: {e}")
    finally:
        _dispatch_task.cancel()
        try:
            await _dispatch_task
        except asyncio.CancelledError:
            pass
        _dispatch_task = None
        logger.info("Telegram alerter stopped.")


# ---------------------------------------------------------------------------
# Public API — enqueue helpers (all non-blocking, safe to call from anywhere)
# ---------------------------------------------------------------------------


def _enqueue(level: AlertLevel, body: str) -> None:
    """
    Thread-safe enqueue.  Works from asyncio coroutines and sync threads alike.
    """
    text = _build_message(level, body)
    try:
        _queue.put_nowait(text)
    except asyncio.QueueFull:
        # Queue is unbounded — this should never happen, but guard anyway.
        logger.error("Alert queue unexpectedly full. Message dropped:\n%s", text)


# ---------------------------------------------------------------------------
# CRITICAL alerts
# ---------------------------------------------------------------------------


def alert_circuit_breaker_tripped(daily_pnl: float) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>CIRCUIT BREAKER TRIPPED</b>\n"
        f"Daily P&amp;L: <code>₹{daily_pnl:,.2f}</code>\n"
        f"All positions squared off. System locked until 03:10 PM.",
    )


def alert_websocket_dead(consecutive_misses: int, last_tick_age_secs: float) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>WEBSOCKET DEAD</b>\n"
        f"Consecutive missed pongs: <code>{consecutive_misses}</code>\n"
        f"Last tick age: <code>{last_tick_age_secs:.1f}s</code>\n"
        f"Reconnect sequence initiated.",
    )


def alert_auth_failed(reason: str) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>AUTHENTICATION FAILED</b>\n"
        f"Reason: <code>{reason}</code>\n"
        f"System halted. Manual intervention required.",
    )


def alert_iv_bootstrap_failed(records_found: int) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>IV BOOTSTRAP FAILED</b>\n"
        f"Records found in iv_history: <code>{records_found}</code> "
        f"(minimum 200 required)\n"
        f"System in Standby for this session.",
    )


def alert_scrip_master_parse_error(reason: str) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>SCRIP MASTER PARSE ERROR</b>\n"
        f"Reason: <code>{reason}</code>\n"
        f"System halted before market open.",
    )


def alert_premarket_init_failed(failed_step: str, reason: str) -> None:
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>PRE-MARKET INIT FAILED</b>\n"
        f"Step: <code>{failed_step}</code>\n"
        f"Reason: <code>{reason}</code>\n"
        f"System halted.",
    )


def alert_health_watchdog_missed(consecutive_misses: int) -> None:
    """
    Called by health_check.py subprocess via Telegram HTTP directly
    (because the main process may be dead). This function exists so
    reporter tests can call it in Ghost mode — the subprocess uses
    its own direct HTTP call.
    """
    _enqueue(
        AlertLevel.CRITICAL,
        f"<b>HEALTH WATCHDOG: MAIN PROCESS UNRESPONSIVE</b>\n"
        f"Consecutive missed heartbeats: <code>{consecutive_misses}</code>\n"
        f"Main bot may have crashed or hung.",
    )


# ---------------------------------------------------------------------------
# WARNING alerts
# ---------------------------------------------------------------------------


def alert_basket_rejected(strikes: str, reason: str) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>BASKET ORDER REJECTED</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Reason: <code>{reason}</code>\n"
        f"Position slot freed.",
    )


def alert_expiry_weekday_changed(previous: str, current: str, expiry_date: str) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>EXPIRY WEEKDAY CHANGED</b>\n"
        f"Previous session: <code>{previous}</code>\n"
        f"This session: <code>{current}</code>\n"
        f"Expiry date: <code>{expiry_date}</code>\n"
        f"Token map rebuilt. Verify Scrip Master manually.",
    )


def alert_margin_insufficient(
    required: float, available: float, trade_desc: str
) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>MARGIN PRE-CHECK FAILED — TRADE SKIPPED</b>\n"
        f"Trade: <code>{trade_desc}</code>\n"
        f"Required: <code>₹{required:,.2f}</code> "
        f"(×1.2 buffer = <code>₹{required * 1.2:,.2f}</code>)\n"
        f"Available: <code>₹{available:,.2f}</code>",
    )


def alert_rate_staleness(field: str, days_since_verified: int, level: str) -> None:
    """level: 'WARNING' | 'HIGH' | 'CRITICAL'"""
    _enqueue(
        AlertLevel.WARNING,
        f"<b>TRANSACTION COST RATE STALE — {level}</b>\n"
        f"Field: <code>{field}</code>\n"
        f"Days since last verification: <code>{days_since_verified}</code>\n"
        f"Verify at NSE/SEBI circulars and update constants.py.",
    )


def alert_rfr_stale(days_since_updated: int) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>RISK-FREE RATE STALE</b>\n"
        f"Days since last update: <code>{days_since_updated}</code>\n"
        f"Update DEFAULT_RISK_FREE_RATE from latest RBI T-bill auction.\n"
        f"rbi.org.in → Press Releases → Treasury Bill Auction Results",
    )


def alert_worker_result_stale(age_secs: float) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>ANALYTICS WORKER RESULT STALE</b>\n"
        f"Result age: <code>{age_secs:.1f}s</code> (threshold: 2s)\n"
        f"Gate 3 suppressed. Check worker process CPU load.",
    )


def alert_insufficient_strike_pairs(found: int, minimum: int = 20) -> None:
    _enqueue(
        AlertLevel.WARNING,
        f"<b>SCRIP MASTER: INSUFFICIENT STRIKE PAIRS</b>\n"
        f"Valid pairs found: <code>{found}</code> "
        f"(minimum: <code>{minimum}</code>)\n"
        f"Possible Scrip Master schema change. System halted.",
    )


def alert_macro_event_blackout(
    event_desc: str, blackout_type: str, minutes: int
) -> None:
    """blackout_type: 'PRE' | 'POST'"""
    _enqueue(
        AlertLevel.INFO,
        f"<b>MACRO BLACKOUT — {blackout_type}-EVENT</b>\n"
        f"Event: <code>{event_desc}</code>\n"
        f"New entries blocked for <code>{minutes}</code> minutes.",
    )


# ---------------------------------------------------------------------------
# INFO alerts
# ---------------------------------------------------------------------------


def alert_trade_entry(
    direction: str,
    strategy: str,
    strikes: str,
    lots: int,
    net_premium: float,
    regime: str,
) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"<b>TRADE ENTRY — {direction} {strategy}</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Lots: <code>{lots}</code>  "
        f"Net premium: <code>₹{net_premium:,.2f}</code>\n"
        f"Regime: <code>{regime}</code>",
    )


def alert_trade_exit(
    direction: str,
    strategy: str,
    strikes: str,
    net_pnl: float,
    exit_reason: str,
) -> None:
    emoji = "✅" if net_pnl >= 0 else "❌"
    _enqueue(
        AlertLevel.INFO,
        f"{emoji} <b>TRADE EXIT — {direction} {strategy}</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Net P&amp;L: <code>₹{net_pnl:,.2f}</code>\n"
        f"Reason: <code>{exit_reason}</code>",
    )


def alert_stop_loss_triggered(
    strikes: str, spread_value: float, threshold: float
) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"🛑 <b>STOP-LOSS TRIGGERED</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Spread MTM: <code>₹{spread_value:,.2f}</code>  "
        f"Threshold: <code>₹{threshold:,.2f}</code>",
    )


def alert_theta_stagnation_exit(
    strikes: str, minutes_held: int, gain_pct: float
) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"⏱️ <b>THETA STAGNATION EXIT</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Held: <code>{minutes_held} min</code>  "
        f"Gain: <code>{gain_pct * 100:.1f}%</code> (threshold: 15%)",
    )


def alert_spread_roll_executed(
    old_short: str, new_short: str, net_credit: float
) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"🔄 <b>SPREAD ROLL EXECUTED</b>\n"
        f"Old short: <code>{old_short}</code>\n"
        f"New short: <code>{new_short}</code>\n"
        f"Net credit after costs: <code>₹{net_credit:,.2f}</code>",
    )


def alert_spread_roll_unavailable(strikes: str, reason: str) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"🔄 <b>SPREAD ROLL UNAVAILABLE — CLOSING OUTRIGHT</b>\n"
        f"Strikes: <code>{strikes}</code>\n"
        f"Reason: <code>{reason}</code>",
    )


def alert_near_expiry_force_close(position_count: int) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"⚠️ <b>NEAR-EXPIRY FORCE CLOSE (01:30 PM)</b>\n"
        f"Positions closed: <code>{position_count}</code>\n"
        f"All positions exited before final settlement window.",
    )


def alert_hard_kill_complete(position_count: int) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"🔒 <b>03:10 PM HARD KILL COMPLETE</b>\n"
        f"Positions closed: <code>{position_count}</code>\n"
        f"WebSocket terminated. Session frozen.",
    )


def alert_signal_quarantined(direction: str, reason: str) -> None:
    _enqueue(
        AlertLevel.INFO,
        f"🕐 <b>SIGNAL QUARANTINED</b>\n"
        f"Direction: <code>{direction}</code>\n"
        f"Reason: <code>{reason}</code>\n"
        f"Will re-evaluate at next execution window.",
    )


# ---------------------------------------------------------------------------
# Generic alert — used by infrastructure modules (database, write_buffer, etc.)
# ---------------------------------------------------------------------------


async def send_alert(message: str) -> None:
    """
    Generic async alert used by all infrastructure modules.
    Classifies automatically based on message prefix:
        "CRITICAL:" → AlertLevel.CRITICAL
        "HIGH:"     → AlertLevel.WARNING
        "WARNING:"  → AlertLevel.WARNING
        anything    → AlertLevel.INFO

    Called with await throughout the codebase, e.g.:
        await send_alert("CRITICAL: Auth failed — ...")
        await send_alert("HIGH: Write buffer queue full — ...")

    Non-blocking: enqueues to the Telegram queue and returns immediately.
    """
    upper = message.upper()
    if upper.startswith("CRITICAL"):
        level = AlertLevel.CRITICAL
    elif upper.startswith("HIGH") or upper.startswith("WARNING"):
        level = AlertLevel.WARNING
    else:
        level = AlertLevel.INFO
    _enqueue(level, message)


# ---------------------------------------------------------------------------
# Raw message (used by reporter.py for EOD report)
# ---------------------------------------------------------------------------


def send_raw(text: str) -> None:
    """
    Enqueue a pre-formatted message verbatim.
    Used by reporter.py to send the EOD report without wrapping.
    Still truncated to Telegram's 4096-char limit.
    """
    _queue.put_nowait(_truncate(text))


# ---------------------------------------------------------------------------
# Synchronous wrapper — for use from non-async contexts (health_check subprocess)
# ---------------------------------------------------------------------------


def send_critical_sync(body: str) -> bool:
    """
    Blocking synchronous send — for the health_check subprocess which has
    no running asyncio loop. Opens a fresh loop, sends, closes.
    Returns True on success.
    """
    text = _build_message(AlertLevel.CRITICAL, body)

    async def _run() -> bool:
        return await _send_with_retry_sync(text)

    async def _send_with_retry_sync(t: str) -> bool:
        for delay in [0] + list(_RETRY_DELAYS):
            if delay:
                await asyncio.sleep(delay)
            if await _send_now(t):
                return True
        return False

    return asyncio.run(_run())
