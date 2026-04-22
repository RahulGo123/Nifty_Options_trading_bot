"""
monitoring/health_check.py
==========================
Health watchdog — runs as a fully independent subprocess.

Architecture
------------
This module is launched by main.py as a separate subprocess using
multiprocessing.Process. It has NO imports from the main bot package
at runtime — it is self-contained so it survives a main process crash,
hang, or OOM kill.

Protocol
--------
The main process increments a shared multiprocessing.Value counter
(the "heartbeat counter") at least once every WATCHDOG_INTERVAL seconds.
The watchdog snapshots that counter every WATCHDOG_INTERVAL seconds.
If the counter has not changed for WATCHDOG_MISS_THRESHOLD consecutive
snapshots, the watchdog fires a Telegram CRITICAL alert via its own
direct HTTP call (NOT via alerting.py — that lives in the main process).

Startup
-------
Call start_watchdog(heartbeat_counter) from main.py:

    from monitoring.health_check import start_watchdog
    import multiprocessing

    _heartbeat = multiprocessing.Value("i", 0)
    watchdog_proc = start_watchdog(_heartbeat)

Heartbeat increment (call from main event loop):

    from monitoring.health_check import increment_heartbeat
    increment_heartbeat(_heartbeat)

The watchdog subprocess exits cleanly when the main process dies
because it daemonises itself (daemon=True).
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import sys
import time
from ctypes import c_int
from multiprocessing.sharedctypes import Synchronized
from typing import Optional

# requests is used inside the subprocess directly — no dependency on alerting.py.
# The subprocess has no asyncio event loop, so synchronous requests is correct.
import requests
from decouple import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (read inside subprocess — decouple re-reads .env)
# ---------------------------------------------------------------------------

WATCHDOG_INTERVAL: int = config("WATCHDOG_INTERVAL", default=30, cast=int)
WATCHDOG_MISS_THRESHOLD: int = config("WATCHDOG_MISS_THRESHOLD", default=2, cast=int)


# ---------------------------------------------------------------------------
# Subprocess entrypoint (self-contained — no main-process imports at runtime)
# ---------------------------------------------------------------------------


def _watchdog_loop(
    heartbeat_counter: Synchronized,  # type: ignore[type-arg]
    bot_token: str,
    chat_id: str,
    mode_tag: str,
    interval: int,
    miss_threshold: int,
) -> None:
    """
    The actual watchdog loop running inside the subprocess.
    All arguments are primitives or shared-memory objects — no pickling of
    complex objects.
    """
    # Set up minimal logging for the subprocess.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [WATCHDOG] %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("health_check.subprocess")
    log.info("Watchdog subprocess started. PID=%d", os.getpid())

    last_seen: int = heartbeat_counter.value
    consecutive_misses: int = 0
    alert_sent_this_miss_run: bool = False

    while True:
        time.sleep(interval)

        current: int = heartbeat_counter.value

        if current != last_seen:
            # Main process is alive.
            if consecutive_misses > 0:
                log.info(
                    "Heartbeat resumed after %d missed check(s). Counter=%d",
                    consecutive_misses,
                    current,
                )
            consecutive_misses = 0
            alert_sent_this_miss_run = False
            last_seen = current
            continue

        # Counter has not advanced.
        consecutive_misses += 1
        log.warning(
            "Heartbeat NOT incremented. Miss #%d (threshold=%d). Counter=%d",
            consecutive_misses,
            miss_threshold,
            current,
        )

        if consecutive_misses >= miss_threshold and not alert_sent_this_miss_run:
            log.error(
                "Threshold reached — firing Telegram CRITICAL alert. "
                "Consecutive misses=%d",
                consecutive_misses,
            )
            _fire_critical_alert(
                bot_token=bot_token,
                chat_id=chat_id,
                mode_tag=mode_tag,
                consecutive_misses=consecutive_misses,
            )
            alert_sent_this_miss_run = True  # One alert per unresponsive episode.


def _fire_critical_alert(
    bot_token: str,
    chat_id: str,
    mode_tag: str,
    consecutive_misses: int,
) -> None:
    """
    Direct synchronous Telegram HTTP call from the watchdog subprocess.
    Does NOT use alerting.py (that lives in the main process).
    Retries up to 3 times before giving up.
    """
    text = (
        f"{mode_tag} 🚨 CRITICAL\n"
        f"🕐 {time.strftime('%H:%M:%S')}\n\n"
        f"<b>HEALTH WATCHDOG: MAIN PROCESS UNRESPONSIVE</b>\n"
        f"Consecutive missed heartbeats: <code>{consecutive_misses}</code>\n"
        f"Main bot may have crashed or hung.\n"
        f"Watchdog PID: <code>{os.getpid()}</code>"
    )
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    log = logging.getLogger("health_check.subprocess")

    for attempt in range(1, 4):
        try:
            resp = requests.post(url, json=payload, timeout=10.0)
            if resp.status_code == 200:
                log.info("Telegram CRITICAL alert sent successfully.")
                return
            log.warning(
                "Telegram returned HTTP %s on attempt %d: %s",
                resp.status_code,
                attempt,
                resp.text[:200],
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Telegram send failed on attempt %d: %s", attempt, exc)
        if attempt < 3:
            time.sleep(5)

    log.error("All Telegram alert attempts exhausted — alert NOT delivered.")


# ---------------------------------------------------------------------------
# Public API — called from main.py
# ---------------------------------------------------------------------------


def start_watchdog(
    heartbeat_counter: Synchronized,  # type: ignore[type-arg]
) -> multiprocessing.Process:
    """
    Spawn and return the watchdog subprocess.

    Parameters
    ----------
    heartbeat_counter : multiprocessing.Value("i", 0)
        Shared integer counter. Main process calls increment_heartbeat()
        to advance it. Watchdog checks it every WATCHDOG_INTERVAL seconds.

    Returns
    -------
    multiprocessing.Process
        The subprocess handle. main.py should store it but NOT join() it —
        it runs indefinitely.
    """
    bot_token: str = config("TELEGRAM_BOT_TOKEN", default="")
    chat_id: str = config("TELEGRAM_CHAT_ID", default="")
    live_trading: bool = config("LIVE_TRADING", default=False, cast=bool)
    mode_tag = "[🔴 LIVE]" if live_trading else "[👻 GHOST]"
    interval: int = config("WATCHDOG_INTERVAL", default=30, cast=int)
    miss_threshold: int = config("WATCHDOG_MISS_THRESHOLD", default=2, cast=int)

    proc = multiprocessing.Process(
        target=_watchdog_loop,
        args=(
            heartbeat_counter,
            bot_token,
            chat_id,
            mode_tag,
            interval,
            miss_threshold,
        ),
        name="HealthWatchdog",
        daemon=True,  # Dies automatically when main process exits.
    )
    proc.start()
    logger.info(
        "Health watchdog subprocess started. PID=%d interval=%ds threshold=%d",
        proc.pid,
        interval,
        miss_threshold,
    )
    return proc


def increment_heartbeat(heartbeat_counter: Synchronized) -> None:  # type: ignore[type-arg]
    """
    Increment the shared heartbeat counter.

    Call this from the main asyncio event loop at regular intervals —
    e.g., via APScheduler every WATCHDOG_INTERVAL seconds, or from
    the WebSocket tick callback.

    Thread-safe: multiprocessing.Value uses a lock internally.
    """
    with heartbeat_counter.get_lock():
        # Wrap at sys.maxsize to prevent overflow on long-running sessions.
        heartbeat_counter.value = (heartbeat_counter.value + 1) % (2**30)


def make_heartbeat_counter() -> Synchronized:  # type: ignore[type-arg]
    """
    Convenience factory. Call from main.py before spawning the watchdog.

    Usage
    -----
        heartbeat = make_heartbeat_counter()
        watchdog_proc = start_watchdog(heartbeat)
        # Later, inside event loop:
        increment_heartbeat(heartbeat)
    """
    return multiprocessing.Value(c_int, 0)


# ---------------------------------------------------------------------------
# Standalone entry point — for testing the watchdog in isolation
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Run this file directly to test the watchdog without the full bot:

        python -m monitoring.health_check

    The main thread increments the counter for 30 seconds, then stops.
    You should receive a Telegram CRITICAL alert ~60 seconds after it stops.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Health watchdog smoke test")
    parser.add_argument(
        "--live-seconds",
        type=int,
        default=30,
        help="How many seconds to simulate a live main process before going silent",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    hb = make_heartbeat_counter()
    proc = start_watchdog(hb)

    log.info("Simulating live main process for %d seconds...", args.live_seconds)
    for _ in range(args.live_seconds):
        time.sleep(1)
        increment_heartbeat(hb)

    log.info(
        "Main process gone silent. Watchdog should alert in ~%ds.",
        WATCHDOG_INTERVAL * WATCHDOG_MISS_THRESHOLD,
    )

    # Keep the script alive so the watchdog has time to fire.
    timeout = WATCHDOG_INTERVAL * (WATCHDOG_MISS_THRESHOLD + 2)
    log.info("Waiting %d seconds for watchdog to fire...", timeout)
    time.sleep(timeout)

    proc.terminate()
    log.info("Test complete.")
