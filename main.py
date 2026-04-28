"""
main.py
=======
System entry point.

Startup sequence
----------------
0. Logging initialised (loguru to file + console)
1. Watchdog subprocess spawned (independent — survives main process crash)
2. Alerter started (Telegram queue dispatcher)
3. Database pool created
4. Write buffer started
5. Scheduler started (APScheduler registers all daily jobs)
6. Event loop runs until the scheduler fires the 03:10 PM hard kill,
   at which point the scheduled hard_kill job performs full shutdown
   and the event loop exits cleanly.

Shutdown (triggered by 03:10 PM hard kill or SIGTERM/SIGINT)
-------------------------------------------------------------
1. Signal evaluation loop cancelled
2. Lifecycle manager stops (positions already closed by hard_kill job)
3. WebSocket severed
4. Analytics worker process terminated
5. Write buffer drained (final flush)
6. Database pool closed
7. APScheduler stopped
8. Watchdog subprocess exits automatically (daemon=True)

Run
---
    python main.py                    # Ghost Mode (default)
    LIVE_TRADING=True python main.py  # Live Mode (with capital)

Environment
-----------
All secrets and configuration are in .env.
See config/settings.py for the full list of required variables.

The system is designed to be started fresh each morning.
Do not restart it during market hours — the 08:45 pre-market
init runs exactly once per session.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import signal
import sys
from datetime import datetime, date, time
from pathlib import Path
import os
from loguru import logger
from decouple import config


# ---------------------------------------------------------------------------
# Logging setup — must happen before any project imports
# ---------------------------------------------------------------------------


def _configure_logging() -> None:
    """
    loguru: console (INFO+) and rotating daily file (DEBUG+).
    Log directory: logs/ relative to project root.
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "bot_{time:YYYY-MM-DD}.log"

    # Remove default loguru handler
    logger.remove()

    # Console — INFO and above, coloured
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:HH:mm:ss}</green> "
            "<level>{level:<8}</level> "
            "<cyan>{name}</cyan> — {message}"
        ),
        colorize=True,
    )

    # File — DEBUG and above, rotation daily, retention 7 days, zip old logs
    logger.add(
        str(log_dir / "trading_bot.log"),
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} {level:<8} {name} — {message}",
        rotation="00:00",  # Rotate at midnight
        retention="7 days",
        compression="gz",
        enqueue=True,
    )

    logger.info(f"Logging initialised. Log dir: {log_dir}")


_configure_logging()


# ---------------------------------------------------------------------------
# Project imports — after logging is configured
# ---------------------------------------------------------------------------

from monitoring.alerting import start_alerter, send_alert
from monitoring.health_check import make_heartbeat_counter, start_watchdog
from persistence.database import get_pool, close_pool
from persistence.models import ALL_TABLES, CREATE_INDEXES
from persistence.write_buffer import write_buffer
from scheduler.market_scheduler import market_scheduler
from monitoring.telegram_listener import telegram_listener


# ---------------------------------------------------------------------------
# Live / Ghost mode banner
# ---------------------------------------------------------------------------

LIVE_TRADING: bool = config("LIVE_TRADING", default=False, cast=bool)

_BANNER = (
    "\n"
    "╔══════════════════════════════════════════════════════╗\n"
    "║    Nifty Options Bot — Multi-Factor Volatility Regime  ║\n"
    "║    Mode: {mode:<46} ║\n"
    "║    Started: {started:<43} ║\n"
    "╚══════════════════════════════════════════════════════╝"
)


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------


async def _ensure_schema() -> None:
    """
    Creates all tables and indexes if they don't already exist.
    Safe to run on every startup — all statements use IF NOT EXISTS.
    """
    from persistence.database import execute

    logger.info("[MAIN] Ensuring database schema...")
    for create_stmt in ALL_TABLES:
        await execute(create_stmt)

    # Indexes are in a single block — execute splits on semicolons
    for stmt in CREATE_INDEXES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                await execute(stmt)
            except Exception as exc:
                # Indexes failing is non-fatal — log and continue
                logger.warning("[MAIN] Index creation warning: %s", exc)

    logger.info("[MAIN] Database schema ready.")


# ---------------------------------------------------------------------------
# Signal handling (SIGTERM / SIGINT)
# ---------------------------------------------------------------------------

# _shutdown_event is created inside main() after the event loop is running.
# It cannot be a module-level variable because asyncio.Event() must be
# created inside a running event loop (Python 3.10+ enforces this).
_shutdown_event: asyncio.Event | None = None


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """
    SIGTERM (systemd stop, Docker stop) and SIGINT (Ctrl+C) both
    set the shutdown event, which triggers graceful shutdown.
    """

    def _handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning("[MAIN] Received %s — initiating graceful shutdown.", sig_name)
        if _shutdown_event is not None:
            loop.call_soon_threadsafe(_shutdown_event.set)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    logger.info("[MAIN] Signal handlers installed (SIGTERM, SIGINT).")


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------


async def _shutdown() -> None:
    """
    Called when _shutdown_event is set (SIGTERM/SIGINT) or after the
    03:10 PM hard kill job completes.

    The hard kill job handles position closure and write buffer drain
    internally. This function handles the remaining infrastructure cleanup.
    """
    logger.info("[MAIN] Graceful shutdown sequence starting...")

    # Stop the scheduler (cancels all pending jobs)
    try:
        market_scheduler.stop()
    except Exception as exc:
        logger.warning("[MAIN] Scheduler stop error: %s", exc)

    # Close database write buffer (flushes remaining records)
    try:
        from persistence.write_buffer import write_buffer

        await write_buffer.stop()
    except Exception as exc:
        logger.warning("[MAIN] Write buffer stop error: %s", exc)

    # Close database pool
    try:
        await close_pool()
        logger.info("[MAIN] Database pool closed.")
    except Exception as exc:
        logger.warning("[MAIN] Database pool close error: %s", exc)

    # Close Telegram Alerter (drains final reports)
    try:
        from monitoring.alerting import stop_alerter

        await stop_alerter()
    except Exception as exc:
        logger.warning("[MAIN] Alerter stop error: %s", exc)

    # --- THE PROCESS REAPER: Kill Zombie Subprocesses ---
    import multiprocessing

    active_children = multiprocessing.active_children()
    if active_children:
        logger.info(f"[MAIN] Cleaning up {len(active_children)} child processes...")
        for child in active_children:
            logger.info(f"[MAIN] Terminating {child.name} (PID: {child.pid})")
            child.terminate()
            # Wait up to 2s for clean exit, then move on
            child.join(timeout=2)
    # ----------------------------------------------------

    logger.info("[MAIN] Shutdown complete.")
    # Final 1s grace sleep for any lingering OS file descriptors
    await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    """
    Async entry point. Runs for the full trading session.
    Exits after the 03:10 PM hard kill or on SIGTERM/SIGINT.
    """
    global _shutdown_event
    _shutdown_event = asyncio.Event()  # Created inside running event loop

    loop = asyncio.get_running_loop()

    # ── Mode banner ──
    mode_str = (
        "🔴 LIVE TRADING — REAL CAPITAL AT RISK"
        if LIVE_TRADING
        else "👻 GHOST MODE — Simulation (no capital)"
    )
    logger.info(
        _BANNER.format(
            mode=mode_str,
            started=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
    )

    if LIVE_TRADING:
        logger.warning(
            "[MAIN] ⚠️  LIVE TRADING IS ACTIVE. "
            "Real capital will be deployed. "
            "Ensure DHAN_ACCESS_TOKEN is updated in .env for today's session."
        )

    # ── Step 1: Watchdog subprocess (independent — spawns before event loop work) ──
    logger.info("[MAIN] Starting health watchdog subprocess...")
    heartbeat_counter = make_heartbeat_counter()
    watchdog_proc = start_watchdog(heartbeat_counter)
    logger.info(f"Watchdog PID: {os.getpid()}")

    # ── Step 2: Alerter (Telegram queue dispatcher) ──
    start_alerter()
    await send_alert(
        f"INFO: Bot starting. "
        f"Mode: {'LIVE' if LIVE_TRADING else 'GHOST'} | "
        f"Time: {datetime.now().strftime('%H:%M:%S')}"
    )

    # ── Step 3: Database pool ──
    logger.info("[MAIN] Creating database connection pool...")
    try:
        await get_pool()
        await _ensure_schema()
    except Exception as exc:
        logger.critical("[MAIN] Database startup failed: %s. Cannot continue.", exc)
        await send_alert(f"CRITICAL: Database startup failed — {exc}. Bot aborted.")
        return

    # ── Step 4: Write buffer ──
    write_buffer.start()
    logger.info("[MAIN] Write buffer started.")

    # ── Step 5: Install signal handlers ──
    _install_signal_handlers(loop)

    # ── Step 6: Inject dependencies into scheduler, then start ──
    from auth.dhan_auth import auth_engine
    from positions.lifecycle_manager import lifecycle_manager

    market_scheduler.heartbeat_counter = heartbeat_counter
    market_scheduler.shutdown_event = _shutdown_event
    market_scheduler.auth_engine = auth_engine
    market_scheduler.lifecycle_manager = lifecycle_manager

    await telegram_listener.start()
    market_scheduler.start()
    logger.info("[MAIN] Scheduler started. Waiting for 08:45 pre-market init...")

    now = datetime.now()
    current_time = now.time()

    # If the bot boots between 08:40 AM and 03:30 PM, it should recover positions immediately
    if time(8, 40) <= current_time < time(15, 35):
        logger.warning(
            f"[MAIN] Session window active ({current_time.strftime('%H:%M')}). Executing Boot Recovery sequence..."
        )

        # 1. Force the 08:45 AM Pre-Market Bootstrap natively
        try:
            await market_scheduler._job_premarket_init()
            logger.info("[MAIN] Boot Recovery: Session state and positions recovered.")
        except Exception as e:
            logger.error(f"[MAIN] Hot Boot pre-market failed: {e}")

        # 2. Give it 5 seconds to download the IVR history and build the gap baseline
        await asyncio.sleep(5)

        # 3. Force the 09:15 AM Market Open (WebSocket Connection) natively
        # 3. Force the 09:15 AM Market Open (WebSocket Connection) natively
        try:
            await market_scheduler._job_market_open()
            logger.info("[MAIN] Hot Boot: Market Open WebSocket connection triggered.")
        except Exception as e:
            logger.error(f"[MAIN] Hot Boot market open failed: {e}")

        # 4. Activate PCR Gate and trigger immediate IVR calculation
        from config.constants import PCR_ACTIVATION_TIME

        if current_time >= PCR_ACTIVATION_TIME:
            try:
                await market_scheduler._job_activate_pcr()
                logger.info("[MAIN] Hot Boot: PCR Gate manually activated.")

                # TRIGGER IVR & PCR INITIAL SNAPSHOTS (Don't wait for the cron)
                await market_scheduler._job_ivr_update()
                await market_scheduler._job_oi_wall_update()
                logger.info("[MAIN] Hot Boot: Initial IVR and PCR snapshots captured.")
                # --- NEW: Immediate Exit if past specific cutoffs ---
                is_expiry = False
                try:
                    from data.runtime_config import runtime_config

                    nifty = runtime_config.instruments.get("NIFTY")
                    is_expiry = nifty and nifty.next_expiry == date.today()
                except:
                    pass

                # Handle 1:30 PM Expiry Exit recovery
                if is_expiry and current_time >= time(13, 30):
                    logger.warning(
                        "[MAIN] Recovery: Past 01:30 PM on Expiry. Triggering exit check..."
                    )
                    await market_scheduler._job_expiry_exit()

                # Handle 3:10 PM Hard Kill recovery
                elif current_time >= time(15, 10):
                    logger.warning(
                        "[MAIN] Recovery: Past 03:10 PM. Triggering final liquidation..."
                    )
                    await market_scheduler._job_eod_reporting_and_kill()

            except Exception as e:
                logger.error(f"[MAIN] Hot Boot PCR activation failed: {e}")

    else:
        logger.info("[MAIN] Session window expired or pre-market standby.")

    # ── Run until shutdown event (SIGTERM) or APScheduler fires the hard kill ──
    # The 03:10 PM hard kill job calls write_buffer.stop() and sets the session
    # as ended. The main loop here just keeps the event loop alive until then.
    await _wait_for_session_end()

    # ── Cleanup ──
    await _shutdown()


async def _wait_for_session_end() -> None:
    """
    Keeps the event loop alive until shutdown is triggered.
    Prints a full dashboard every 5 minutes — not every 30 seconds.
    """
    _tick = 0
    while True:
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=30.0)
            break  # Shutdown event set — exit
        except asyncio.TimeoutError:
            _tick += 1
            if _tick >= 2:  # 2 × 30s = 1 minute
                _log_health_status()
                _tick = 0
            continue


def _log_health_status() -> None:
    """Full dashboard — printed every 5 minutes to console. Not sent to Telegram."""
    from risk.portfolio_state import portfolio_state
    from analytics.ivr_engine import ivr_engine
    from analytics.vwap_engine import vwap_engine
    from analytics.atr_engine import atr_engine
    from execution.margin_cache import margin_cache
    from scheduler.market_scheduler import market_scheduler

    try:
        now_str = datetime.now().strftime("%H:%M:%S")
        spot = vwap_engine.last_price
        vwap = vwap_engine.value
        vix = ivr_engine.current_iv
        atr = atr_engine.atr
        margin = margin_cache.get()
        n_pos = portfolio_state.position_count
        upnl = sum(p.unrealized_pnl for p in portfolio_state.open_positions.values())
        rpnl = portfolio_state.realized_pnl
        daily = portfolio_state.daily_pnl

        # Expiry status
        try:
            from data.runtime_config import runtime_config

            nifty = runtime_config.instruments.get("NIFTY")
            exp_str = (
                nifty.next_expiry.strftime("%b %d")
                if nifty and nifty.next_expiry
                else "Unknown"
            )
        except:
            exp_str = "Unknown"

        # Gate status
        vix_gate = "✅" if 11.0 <= vix <= 22.0 else "❌"
        vwap_gate = (
            "✅" if vwap > 0 and spot > 0 and abs(spot - vwap) / vwap <= 0.0045 else "—"
        )
        morn_f = "FIRED" if market_scheduler._morning_fired else "open"
        aftn_f = "FIRED" if market_scheduler._afternoon_fired else "open"
        # Sentiment
        from analytics.pcr_engine import pcr_engine

        pcr = pcr_engine.pcr
        bias = pcr_engine.signal

        # Calculate hedged free capital for dashboard
        try:
            from data.runtime_config import runtime_config

            put_lots = sum(
                p.lots
                for p in portfolio_state.open_positions.values()
                if "PUT" in p.spread_type
            )
            call_lots = sum(
                p.lots
                for p in portfolio_state.open_positions.values()
                if "CALL" in p.spread_type
            )
            margin_lots = max(put_lots, call_lots)
            free_cap = runtime_config.risk.total_capital - (margin_lots * margin)
            free_cap_str = f"₹{free_cap:,.2f}"
        except:
            free_cap_str = "Unknown"

        # RUN RATE CALCULATIONS
        total_theta = sum(p.net_theta for p in portfolio_state.open_positions.values())
        total_delta = sum(p.net_delta for p in portfolio_state.open_positions.values())

        # Theta Velocity: ₹ per minute (24hr basis)
        theta_velocity = (total_theta * 65) / 1440

        # Time remaining in session
        close_time = datetime.combine(date.today(), time(15, 30))
        mins_left = max(1, (close_time - datetime.now()).total_seconds() / 60)

        # Session residual theta (if we assume 1 full daily theta is captured in 375 trading mins)
        # This matches the user's manual calculation logic
        theta_cash_day = total_theta * 65
        earnings_per_min = theta_cash_day / mins_left if mins_left > 0 else 0

        sep = "────────────────────────────────────────────────────────────"
        logger.info(
            f"\n{sep}\n"
            f"  DASHBOARD | {now_str} | Expiry: {exp_str}\n"
            f"{sep}\n"
            f"  Spot:  {spot:>10,.2f}    VWAP:  {vwap:>10,.2f}    Δ-VWAP: {((spot-vwap)/vwap*100) if vwap>0 else 0:+.3f}%\n"
            f"  VIX:   {vix:>10.2f}    IVR:   {ivr_engine.ivr:>10.2f}    ATR:    {atr:.2f}\n"
            f"  PCR:   {pcr:>10.4f}    Bias:  {bias:>10}\n"
            f"  Gates: VIX [{vix_gate}]   VWAP [{vwap_gate}]   Morning: [{morn_f}]   Midday: [{aftn_f}]\n"
            f"{sep}\n"
            f"  RUN RATE:   Θ Velocity: ₹{theta_velocity:>6.2f}/min  |  EOD Target: ₹{earnings_per_min:>6.2f}/min\n"
            f"  EXPOSURE:   Net Delta:  {total_delta:>+7.2f}      |  Net Theta:  {total_theta:>+7.2f}\n"
            f"{sep}\n"
            f"  Positions: {n_pos}   Margin/lot: ₹{margin:,.0f}   Free Capital: {free_cap_str}\n"
            f"  Unrealized P&L : ₹{upnl:>10,.2f}\n"
            f"  Realized P&L   : ₹{rpnl:>10,.2f}\n"
            f"  Daily P&L      : ₹{daily:>10,.2f}\n"
            f"{sep}"
        )

        # Per-position detail if any open
        for pid, pos in portfolio_state.open_positions.items():
            logger.info(
                f"  [{pos.spread_type}] {pos.short_strike}/{pos.long_strike} {pos.option_type} "
                f"| Δ: {pos.net_delta:>+6.2f} | Θ: {pos.net_theta:>+6.2f} | "
                f"uPnL: ₹{pos.unrealized_pnl:>8,.2f} | Stop: ₹{pos.stop_loss_value:>8,.0f}"
            )

    except Exception as e:
        logger.debug(f"[MAIN] Dashboard skipped: {e}")


# ---------------------------------------------------------------------------
# The Quant Calendar (NSE 2026)
# ---------------------------------------------------------------------------
def is_trading_day(check_date: date) -> bool:
    """
    Returns False if the date is a weekend or an official NSE holiday.
    Hardcoded for absolute determinism. Zero API dependencies.
    """
    # 1. Check for Weekends (5 = Saturday, 6 = Sunday)
    if check_date.weekday() >= 5:
        return False

    # 2. Check for Official NSE Holidays (2026 Calendar)
    # Note: Excludes holidays that fall on weekends anyway.
    nse_holidays_2026 = {
        date(2026, 1, 26),  # Republic Day
        date(2026, 3, 20),  # Id-Ul-Fitr (Ramzan Id)
        date(2026, 4, 3),  # Good Friday
        date(2026, 4, 14),  # Dr. Baba Saheb Ambedkar Jayanti
        date(2026, 5, 1),  # Maharashtra Day
        date(2026, 5, 27),  # Bakri Id
        date(2026, 6, 26),  # Muharram
        date(2026, 10, 2),  # Mahatma Gandhi Jayanti
        date(2026, 11, 8),  # Diwali-Laxmi Puja (Muhurat Trading excluded)
        date(2026, 12, 25),  # Christmas
    }

    if check_date in nse_holidays_2026:
        return False

    return True


# ---------------------------------------------------------------------------
# Entry guard
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)

    # --- THE PRE-FLIGHT GATE ---
    today = date.today()
    if not is_trading_day(today):
        logger.info(
            f"[MAIN] {today} is a Market Holiday / Weekend. System shutting down cleanly."
        )
        sys.exit(0)
    # ---------------------------

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("[MAIN] Interrupted by user.")
    except Exception as exc:
        logger.critical(f"[MAIN] Fatal unhandled exception: {exc}")
    finally:
        print("\n" + "═" * 56)
    logger.info("[MAIN] Session terminated. Service exiting cleanly.")
