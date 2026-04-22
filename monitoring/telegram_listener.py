import asyncio
import aiohttp
from loguru import logger
from decouple import config
from scheduler.market_scheduler import market_scheduler
from orders.order_manager import order_manager
from risk.portfolio_state import portfolio_state
from monitoring.alerting import send_alert

TELEGRAM_BOT_TOKEN = config("TELEGRAM_BOT_TOKEN", default="")


class TelegramListener:
    def __init__(self):
        self._running = False
        self._task = None
        self._offset = None

        # --- NEW: CONVERSATIONAL STATE MACHINE ---
        self._awaiting_flatten_confirm = False

    async def start(self):
        if not TELEGRAM_BOT_TOKEN:
            logger.warning(
                "[TELEGRAM LISTENER] No Bot Token found. Kill switch disabled."
            )
            return

        self._running = True
        self._task = asyncio.create_task(
            self._poll_updates(), name="telegram_kill_switch"
        )
        logger.info("[TELEGRAM LISTENER] Telegram commands armed (/PANIC, /FLATTEN).")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def _execute_panic_protocol(self):
        """The Nuclear Option. No questions asked."""
        logger.critical("☢️ [TELEGRAM] NUCLEAR PROTOCOL INITIATED!")
        await send_alert(
            "☢️ <b>PANIC PROTOCOL INITIATED</b>\nFlattening all open positions and halting system..."
        )

        market_scheduler._market_open = False

        open_positions = portfolio_state.open_positions
        for pos_id, pos in open_positions.items():
            logger.critical(f"[PANIC] Force closing {pos_id}...")
            await order_manager.close_position(
                position_id=pos_id, market_data={}, exit_reason="MANUAL_PANIC_KILL"
            )
            await asyncio.sleep(1)

        await market_scheduler._job_hard_kill()
        await send_alert("✅ <b>SYSTEM DEAD</b>\nAll positions liquidated.")

    async def _handle_flatten_request(self):
        """Calculates live P&L and asks for confirmation."""
        # Calculate what the P&L would be if we close right now
        current_daily_pnl = portfolio_state.daily_pnl
        open_count = portfolio_state.position_count

        if open_count == 0:
            await send_alert("ℹ️ <b>FLATTEN REJECTED</b>\nYou have no open positions.")
            return

        # Lock the state machine into confirmation mode
        self._awaiting_flatten_confirm = True

        pnl_str = (
            f"🟢 +₹{current_daily_pnl:,.2f}"
            if current_daily_pnl > 0
            else f"🔴 -₹{abs(current_daily_pnl):,.2f}"
        )

        alert_msg = (
            f"⚠️ <b>FLATTEN CONFIRMATION</b>\n"
            f"You have {open_count} open position(s).\n"
            f"Estimated Session P&L: <b>{pnl_str}</b>\n\n"
            f"Type <b>YES</b> to liquidate and shutdown, or <b>NO</b> to abort."
        )
        await send_alert(alert_msg)

    async def _poll_updates(self):
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"

        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    params = {"timeout": 10, "allowed_updates": ["message"]}
                    if self._offset:
                        params["offset"] = self._offset

                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            for result in data.get("result", []):
                                self._offset = result["update_id"] + 1

                                message = result.get("message", {})
                                text = message.get("text", "").strip().upper()

                                # --- STATE MACHINE ROUTING ---
                                if self._awaiting_flatten_confirm:
                                    if text == "YES":
                                        self._awaiting_flatten_confirm = False
                                        await self._execute_panic_protocol()
                                    elif text == "NO":
                                        self._awaiting_flatten_confirm = False
                                        await send_alert(
                                            "✅ <b>FLATTEN ABORTED</b>\nSystem is continuing normal operations."
                                        )
                                    else:
                                        await send_alert(
                                            "⚠️ Invalid response. Type <b>YES</b> or <b>NO</b>."
                                        )
                                    continue  # Skip checking other commands

                                # --- NORMAL COMMAND ROUTING ---
                                if text == "/PANIC":
                                    # Instant death. No confirmation.
                                    await self._execute_panic_protocol()

                                elif text == "/FLATTEN":
                                    # Graceful exit with confirmation.
                                    await self._handle_flatten_request()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug(f"[TELEGRAM LISTENER] Polling error: {e}")

                await asyncio.sleep(3)


telegram_listener = TelegramListener()
