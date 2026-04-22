import json
import time
from loguru import logger
from data.runtime_config import runtime_config


class TickDispatcher:
    """
    The only logic that runs inside the WebSocket on_message handler.

    Rules:
    - Zero computation
    - Zero state mutation
    - Zero logging of individual ticks (too slow)
    - Only classify the tick and route it to the correct queue

    If any queue is full, the OLDEST item is discarded
    and the new tick takes its place. This ensures the
    system always has the most recent data, never stale data.
    """

    # TODO: Verify tick field names against MO API docs.
    # These are assumed field names based on common broker formats.
    TOKEN_FIELD = "token"
    LTP_FIELD = "ltp"
    BID_FIELD = "best_bid_price"
    ASK_FIELD = "best_ask_price"
    OI_FIELD = "oi"
    VOLUME_FIELD = "volume"
    TIMESTAMP_FIELD = "exchange_timestamp"

    def __init__(self):
        self._overflow_count = 0
        self._tick_count = 0
        self._last_log_time = time.monotonic()
        self._options_token_set: set = set()
        self._futures_token: int = None
        self._token_to_instrument: dict = {}  # token → (strike, option_type)

    def build_token_index(self):
        """
        Called once after RuntimeConfig is populated.
        Builds O(1) lookup sets for token routing.
        Replaces the O(n) loop in _is_options_token.
        """
        nifty = runtime_config.instruments.get("NIFTY")
        if not nifty:
            return

        self._futures_token = nifty.futures_token

        for strike, pair in nifty.instrument_tokens.items():
            ce_token = pair["CE"]
            pe_token = pair["PE"]
            self._options_token_set.add(ce_token)
            self._options_token_set.add(pe_token)
            self._token_to_instrument[ce_token] = (strike, "CE")
            self._token_to_instrument[pe_token] = (strike, "PE")

        logger.info(
            f"[TICK DISPATCHER] Token index built: "
            f"{len(self._options_token_set)} options tokens | "
            f"Futures token: {self._futures_token}"
        )

    async def dispatch(self, raw_message: str):
        """
        Receives a raw WebSocket message string.
        Parses it and routes to the correct queue.
        Called on every single incoming message.
        Must be as fast as possible.
        """
        try:
            tick = self._parse(raw_message)
            if tick is None:
                return

            self._tick_count += 1
            token = tick.get(self.TOKEN_FIELD)

            if token is None:
                return

            nifty = runtime_config.instruments.get("NIFTY")
            if not nifty:
                return

            # Route to correct queue based on token type
            if self._is_futures_token(token, nifty):
                await self._put(runtime_config.futures_queue, tick, "futures")

            elif self._is_options_token(token, nifty):
                await self._put(runtime_config.options_queue, tick, "options")

            elif token == "21":
                await self._put(runtime_config.futures_queue, tick, "futures")

            # Log throughput every 60 seconds
            self._log_throughput()

        except Exception as e:
            # Never let a single bad tick crash the dispatcher
            logger.warning(f"[TICK DISPATCHER] Error processing tick: {e}")

    def _parse(self, raw_message) -> dict:
        """
        Parses the raw WebSocket message into a dict.
        The Dhan SDK's get_data() returns pre-parsed dicts, so handle both cases.
        """
        if isinstance(raw_message, dict):
            return raw_message
        try:
            return json.loads(raw_message)
        except (json.JSONDecodeError, TypeError):
            return None

    def _is_futures_token(self, token: int, nifty) -> bool:
        return token == self._futures_token

    def _is_options_token(self, token: int, nifty) -> bool:
        return token in self._options_token_set

    async def _put(self, queue: "asyncio.Queue", tick: dict, queue_name: str):
        """
        Puts a tick into the queue.
        If the queue is full, discards the OLDEST item first
        then puts the new tick. This ensures freshness — the
        system always processes the most recent data.
        Never blocks the event loop waiting for space.
        """
        if queue is None:
            return

        if queue.full():
            try:
                queue.get_nowait()  # Discard oldest
                self._overflow_count += 1
            except Exception:
                pass

        try:
            queue.put_nowait(tick)
        except Exception as e:
            logger.warning(
                f"[TICK DISPATCHER] Failed to put tick " f"in {queue_name} queue: {e}"
            )

    def _log_throughput(self):
        """
        Logs tick throughput once per minute.
        Avoids per-tick logging which would itself block the loop.
        """
        now = time.monotonic()
        elapsed = now - self._last_log_time

        if elapsed >= 60:
            rate = self._tick_count / elapsed
            logger.info(
                f"[TICK DISPATCHER] Throughput: "
                f"{rate:.1f} ticks/sec | "
                f"Total: {self._tick_count:,} | "
                f"Overflows: {self._overflow_count}"
            )
            self._tick_count = 0
            self._overflow_count = 0
            self._last_log_time = now
