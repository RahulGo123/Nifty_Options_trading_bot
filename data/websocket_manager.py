"""
data/websocket_manager.py
=========================
DhanHQ Live Market Feed WebSocket Manager.
Bridges Dhan's synchronous thread-based callbacks into the asyncio event loop.
"""

import asyncio
import time
from datetime import datetime
from loguru import logger
from dhanhq import marketfeed
from config.constants import STALE_FEED_THRESHOLD
from data.runtime_config import runtime_config
from data.tick_dispatcher import TickDispatcher
from monitoring.alerting import send_alert


class WebSocketManager:
    def __init__(self, auth_engine):
        self.auth_engine = auth_engine
        self.dispatcher = TickDispatcher()
        self._feed = None
        self._running = False
        self._loop = None

        # --- WATCHDOG TRACKERS ---
        self.last_tick_time: float = time.time()
        self._watchdog_task: asyncio.Task = None
        self._consumer_task: asyncio.Task = None
        self.is_feed_stale: bool = False
        self.is_connected: bool = False
        self._logged_tick_keys: bool = False

    async def start(self):
        """Entry point. Called at 09:15 AM by the scheduler."""
        runtime_config.init_queues()
        self._running = True
        self._loop = asyncio.get_running_loop()
        logger.info("[WS MANAGER] Starting DhanHQ WebSocket manager...")

        await self._connect_and_run()

        self.is_connected = True
        self.last_tick_time = time.time()  # Reset on fresh connect

        # Arm the Dead-Man's Switch
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(
                self._feed_watchdog(), name="dhan_ws_sentinel"
            )

        # Start queue consumer to drain options/futures queues into market_data_store
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(
                self._consume_queues(), name="tick_queue_consumer"
            )

    async def stop(self):
        """Cleanly severs the WebSocket during the 03:10 PM hard kill."""
        self._running = False
        self.is_connected = False

        if self._watchdog_task and not self._watchdog_task.done():
            self._watchdog_task.cancel()

        if self._consumer_task and not self._consumer_task.done():
            self._consumer_task.cancel()

        if self._feed:
            logger.info("[WS MANAGER] Disconnecting DhanHQ Feed...")
            try:
                await self._feed.disconnect()
            except Exception as e:
                logger.error(f"[WS MANAGER] Error during disconnect: {e}")
            finally:
                self._feed = None

        logger.info("[WS MANAGER] Dhan WebSocket connection closed.")

    def _on_message(self, ws, message):
        """Synchronous callback fired by the DhanHQ SDK thread."""
        # --- UPDATE THE HEARTBEAT ---
        self.last_tick_time = time.time()
        self.is_feed_stale = False
        # ----------------------------

        try:
            if not isinstance(message, dict) or "security_id" not in message:
                return

            ltp = float(message.get("LTP", 0.0))
            best_bid = float(message.get("best_bid_price", 0.0)) or ltp
            best_ask = float(message.get("best_ask_price", 0.0)) or ltp

            mapped_tick = {
                "token": str(message.get("security_id")),
                "ltp": ltp,
                "best_bid_price": best_bid,
                "best_ask_price": best_ask,
                "oi": int(message.get("OI", 0)),
                "volume": int(
                    message.get("traded_volume", 0) or message.get("volume", 0)
                ),
                "exchange_timestamp": time.time(),
            }

            # Use call_soon_threadsafe to jump back into the main loop
            self._loop.call_soon_threadsafe(
                asyncio.create_task, self.dispatcher.dispatch(mapped_tick)
            )
            # Log periodic throughput internally for debugging
            if int(time.time()) % 10 == 0:
                logger.debug(
                    f"[WS DEBUG] Received tick for token: {mapped_tick['token']}"
                )
        except Exception as e:
            logger.debug(f"[WS MANAGER] Dropped malformed tick: {e}")

    def _build_token_list(self) -> list:
        tokens = []
        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            return []
        for strike, pair in nifty.instrument_tokens.items():
            tokens.append(pair["CE"])
            tokens.append(pair["PE"])
        if nifty.futures_token:
            tokens.append(nifty.futures_token)
        return tokens

    async def _connect_and_run(self):
        """Establishes the SDK feed in Full Ticker Mode using the V2 Dictionary schema."""
        tokens = self._build_token_list()
        if not tokens:
            raise ValueError("Token list is empty. Cannot subscribe.")

        self.dispatcher.build_token_index()

        from dhanhq import marketfeed

        # Options in Quote mode to receive OI + Bid/Ask. Futures in Ticker mode (LTP only for VWAP).
        instruments_list = []
        for t in tokens:
            instruments_list.append((marketfeed.NSE_FNO, str(t), marketfeed.Quote))
        # Add INDIA VIX — Ticker only (LTP is enough)
        # Reverting to Segment 0 (NSE) as per scrip master
        instruments_list.append((0, "21", marketfeed.Ticker))

        logger.info(
            f"[WS MANAGER] Hooking {len(instruments_list)} instruments (Options: Quote, VIX: Ticker)..."
        )

        def _thread_runner():
            import logging

            import asyncio  # Important: need it inside the thread context

            # --- THE FIX: Create a dedicated loop for this thread ---
            # This prevents conflicts with the main bot's loop when dhanhq
            # internal calls use run_until_complete.
            thread_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_loop)

            ws_logger = logging.getLogger("dhanhq.marketfeed")
            ws_logger.setLevel(logging.INFO)

            try:
                self._feed = marketfeed.DhanFeed(
                    self.auth_engine.client_id,
                    self.auth_engine.access_token,
                    instruments_list,
                    "v2",
                )

                self._feed.run_forever()  # Connects the WebSocket
                
                self._last_tick_time = time.time()
                self._connected = True
                
                # --- NEW: Recovery Alert ---
                asyncio.run_coroutine_threadsafe(send_alert("⚡ FEED RECOVERED: Dhan WebSocket re-connected. Market data is flowing again."), self._loop)
                logger.info("[WS MANAGER] DhanFeed connected. Entering polling loop.")

                while self._running:
                    try:
                        response = self._feed.get_data()
                        if response:
                            self._on_message(None, response)
                    except Exception as poll_err:
                        logger.warning(f"[WS DIAGNOSTIC] Polling error: {poll_err}")
                        break

                logger.warning("[WS MANAGER] Polling loop exited.")

            except Exception as e:
                logger.error(f"[WS DIAGNOSTIC] Feed crashed: {e}")

        try:
            import threading

            t = threading.Thread(target=_thread_runner, daemon=True)
            t.start()
            logger.info("[WS MANAGER] Background feed thread dispatched.")
        except Exception as e:
            logger.error(f"[WS MANAGER] Thread dispatch failed: {e}")

    async def _consume_queues(self):
        """
        Drains options_queue and futures_queue continuously.
        Maps each tick token back to (strike, option_type) and
        updates market_data_store so prices are always fresh.
        This is the missing consumer that was causing 95% overflow.
        """
        from data.market_data_store import market_data_store

        while self._running:
            try:
                oq = runtime_config.options_queue
                fq = runtime_config.futures_queue

                processed = 0

                if oq:
                    while not oq.empty():
                        tick = oq.get_nowait()
                        if not self._logged_tick_keys:
                            logger.info(
                                f"[WS DEBUG] Quote tick keys: {list(tick.keys())} | Sample: {tick}"
                            )
                            self._logged_tick_keys = True
                        token = tick.get("token")
                        info = self.dispatcher._token_to_instrument.get(token)
                        if info:
                            strike, opt_type = info
                            market_data_store.update(
                                strike=strike,
                                option_type=opt_type,
                                bid=tick.get("best_bid_price", 0.0),
                                ask=tick.get("best_ask_price", 0.0),
                                oi=tick.get("oi", 0),
                                volume=tick.get("volume", 0),
                            )
                        processed += 1

                if fq:
                    from analytics.vwap_engine import vwap_engine
                    from signals.trigger_gate import trigger_gate
                    from analytics.pcr_engine import pcr_engine
                    from datetime import datetime

                    while not fq.empty():
                        tick = fq.get_nowait()
                        token = str(tick.get("token"))

                        if token == "21":
                            market_data_store.update_index(
                                "INDIA_VIX", float(tick.get("ltp", 0.0))
                            )
                        elif token == str(
                            runtime_config.instruments.get("NIFTY").futures_token
                        ):
                            # Nifty Futures tick — feed VWAP, trigger gate, and price store
                            price = float(tick.get("ltp", 0.0))
                            cumvol = float(
                                tick.get("volume", 0)
                            )  # Cumulative daily volume
                            if price > 0:
                                vwap_engine.update(price, cumvol)
                                trigger_gate.on_futures_tick(
                                    price=price,
                                    timestamp=datetime.now(),
                                    direction=pcr_engine.signal,
                                )
                                # Keep market_data_store current so _compute_atm() is live
                                market_data_store.update(
                                    strike=int(token),
                                    option_type="FUT",
                                    bid=price,
                                    ask=price,
                                    oi=int(tick.get("oi", 0)),
                                )
                        processed += 1

                # Yield to event loop; sleep briefly only if queue was empty
                if processed == 0:
                    await asyncio.sleep(0.005)
                else:
                    await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[WS MANAGER] Queue consumer error: {e}")
                await asyncio.sleep(0.01)

    async def _feed_watchdog(self):
        """
        Runs continuously in the background. If no ticks arrive within
        STALE_FEED_THRESHOLD, it triggers a critical alert and reconnects.
        """
        logger.info("[WEBSOCKET] Feed Watchdog armed and monitoring.")

        while self._running:
            await asyncio.sleep(2)

            if not self.is_connected:
                continue

            # Only check for staleness during active market hours
            now = datetime.now().time()
            market_open = datetime.strptime("09:15", "%H:%M").time()
            market_close = datetime.strptime("15:30", "%H:%M").time()

            if market_open <= now <= market_close:
                time_since_last_tick = time.time() - self.last_tick_time

                if (
                    time_since_last_tick > STALE_FEED_THRESHOLD
                    and not self.is_feed_stale
                ):
                    self.is_feed_stale = True
                    logger.critical(
                        f"[WEBSOCKET] DEAD MAN SWITCH TRIGGERED! "
                        f"No data received for {time_since_last_tick:.1f} seconds."
                    )

                    await send_alert(
                        f"🚨 <b>CRITICAL: DATA FEED DEAD</b>\n"
                        f"Stale feed detected ({time_since_last_tick:.0f}s). "
                        f"Stop-Loss engines are blind. Attempting violent reconnect..."
                    )

                    # Violent Reconnect Sequence
                    if self._feed:
                        try:
                            # In v2, disconnect() is an async coroutine.
                            # We just sever the reference and let the background thread die cleanly.
                            self._feed = None
                        except Exception:
                            pass

                    await asyncio.sleep(2)  # Brief pause to let sockets close
                    await self._connect_and_run()

                    # --- THE FIX: Allow subsequent retries if this one fails ---
                    self.is_feed_stale = False
                    self.last_tick_time = (
                        time.time()
                    )  # Reset clock to give it another window


# Single instance
from auth.dhan_auth import auth_engine

ws_manager = WebSocketManager(auth_engine=auth_engine)
