import asyncio
import time
from loguru import logger
from persistence import database
from monitoring.alerting import send_alert


class WriteBuffer:
    """
    Decouples the execution path from database I/O completely.

    How it works:
    - Any module calls await write_buffer.put(table, record)
    - The record goes into an in-memory asyncio.Queue instantly
    - A background drain task wakes every 2 seconds
    - It pulls everything from the queue and writes to
      PostgreSQL in a single executemany batch call
    - The execution path never waits for a database response

    The event loop is never blocked by a database write.
    If PostgreSQL is down, the queue accumulates records
    and drains automatically when the database recovers.
    """

    DRAIN_INTERVAL = 2  # seconds between drain cycles
    MAX_QUEUE_SIZE = 10000  # safety cap — prevents unbounded memory
    ALERT_QUEUE_SIZE = 5000  # alert when queue grows this large

    # SQL INSERT statements for each table
    # Parameters use $1, $2 etc — asyncpg positional format
    INSERT_QUERIES = {
        "trades": """
            INSERT INTO trades (
                session_id, position_id, entry_timestamp, exit_timestamp,
                expiry_date, symbol, long_strike, short_strike,
                option_type, lots, entry_premium_long,
                entry_premium_short, exit_premium_long,
                exit_premium_short, gross_pnl, transaction_costs,
                net_pnl, exit_reason
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18
            )
            ON CONFLICT (position_id) DO UPDATE SET
                exit_timestamp      = EXCLUDED.exit_timestamp,
                exit_premium_long   = EXCLUDED.exit_premium_long,
                exit_premium_short  = EXCLUDED.exit_premium_short,
                gross_pnl           = EXCLUDED.gross_pnl,
                transaction_costs   = EXCLUDED.transaction_costs,
                net_pnl             = EXCLUDED.net_pnl,
                exit_reason         = EXCLUDED.exit_reason
        """,
        "positions": """
            INSERT INTO positions (
                trade_id, position_id, snapshot_timestamp, mark_to_market,
                net_delta, net_theta, net_gamma,
                net_vega, iv_atm
            ) 
            SELECT id, $1, $2, $3, $4, $5, $6, $7, $8
            FROM trades WHERE position_id = $1
        """,
        "iv_history": """
            INSERT INTO iv_history (
                date, symbol, iv_10am, iv_close, ivr_computed
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (date, symbol) DO UPDATE SET
                iv_10am     = EXCLUDED.iv_10am,
                iv_close    = EXCLUDED.iv_close,
                ivr_computed = EXCLUDED.ivr_computed
        """,
        "sessions": """
            INSERT INTO sessions (
                date, starting_capital, ending_pnl,
                trade_count, win_count, loss_count,
                win_rate, ivr_regime, circuit_breaker_triggered
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (date) DO UPDATE SET
                ending_pnl              = EXCLUDED.ending_pnl,
                trade_count             = EXCLUDED.trade_count,
                win_count               = EXCLUDED.win_count,
                loss_count              = EXCLUDED.loss_count,
                win_rate                = EXCLUDED.win_rate,
                ivr_regime              = EXCLUDED.ivr_regime,
                circuit_breaker_triggered = EXCLUDED.circuit_breaker_triggered
        """,
        "runtime_config": """
            INSERT INTO runtime_config (
                config_date, symbol, lot_size, strike_interval,
                expiry_type, next_expiry, expiry_weekday,
                ivr_credit_entry, ivr_credit_exit,
                ivr_debit_entry, ivr_debit_exit,
                pcr_bullish, pcr_bearish, atr_multiplier
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12, $13, $14
            )
            ON CONFLICT (config_date, symbol) DO UPDATE SET
                lot_size            = EXCLUDED.lot_size,
                strike_interval     = EXCLUDED.strike_interval,
                next_expiry         = EXCLUDED.next_expiry,
                expiry_weekday      = EXCLUDED.expiry_weekday,
                ivr_credit_entry    = EXCLUDED.ivr_credit_entry,
                ivr_credit_exit     = EXCLUDED.ivr_credit_exit,
                ivr_debit_entry     = EXCLUDED.ivr_debit_entry,
                ivr_debit_exit      = EXCLUDED.ivr_debit_exit,
                pcr_bullish         = EXCLUDED.pcr_bullish,
                pcr_bearish         = EXCLUDED.pcr_bearish,
                atr_multiplier      = EXCLUDED.atr_multiplier
        """,
    }

    def __init__(self):
        self._queue: asyncio.Queue = None
        self._drain_task: asyncio.Task = None
        self._running: bool = False
        self._total_written: int = 0
        self._total_failed: int = 0

    def start(self):
        """
        Initialises the queue and starts the drain task.
        Called once when the event loop is running.
        Must be called before any put() calls.
        """
        self._queue = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self._running = True
        self._drain_task = asyncio.create_task(
            self._drain_loop(), name="write_buffer_drain"
        )
        logger.info(
            f"[WRITE BUFFER] Started. "
            f"Drain interval: {self.DRAIN_INTERVAL}s. "
            f"Max queue size: {self.MAX_QUEUE_SIZE}."
        )

    async def stop(self):
        """
        Drains any remaining records then stops.
        Called during 03:10 PM hard kill before
        the database pool is closed.
        """
        self._running = False
        if self._drain_task and not self._drain_task.done():
            self._drain_task.cancel()
            try:
                await self._drain_task
            except asyncio.CancelledError:
                pass

        # Final drain — write everything remaining
        if self._queue and not self._queue.empty():
            logger.info("[WRITE BUFFER] Final drain before shutdown...")
            await self._drain_once()
            # Give the network time to finish the physical write to Postgres
            await asyncio.sleep(5)

        logger.info(
            f"[WRITE BUFFER] Stopped. "
            f"Total written: {self._total_written:,}. "
            f"Total failed: {self._total_failed}."
        )

    async def put(self, table: str, record: tuple) -> bool:
        """
        The only method other modules need to call.

        Parameters:
            table:  One of 'trades', 'positions', 'iv_history',
                    'sessions', 'runtime_config'
            record: A tuple of values matching the INSERT query
                    parameter order for that table.

        Returns True if queued successfully.
        Returns False if queue is full (rare — indicates
        drain is not keeping up with writes).

        This method takes microseconds. It never touches
        PostgreSQL directly.
        """
        if self._queue is None:
            logger.error(
                "[WRITE BUFFER] put() called before start(). "
                "Call write_buffer.start() first."
            )
            return False

        if table not in self.INSERT_QUERIES:
            logger.error(
                f"[WRITE BUFFER] Unknown table: '{table}'. "
                f"Valid tables: {list(self.INSERT_QUERIES.keys())}"
            )
            return False

        try:
            self._queue.put_nowait((table, record))

            # Alert if queue is growing too large
            qsize = self._queue.qsize()
            if qsize >= self.ALERT_QUEUE_SIZE:
                logger.warning(
                    f"[WRITE BUFFER] Queue size: {qsize}. "
                    f"Database drain may be falling behind."
                )
            return True

        except asyncio.QueueFull:
            logger.error(
                f"[WRITE BUFFER] Queue full ({self.MAX_QUEUE_SIZE}). "
                f"Record dropped. Check database connectivity."
            )
            await send_alert(
                "HIGH: Write buffer queue full — "
                "records being dropped. "
                "Check PostgreSQL connectivity."
            )
            return False

    async def _drain_loop(self):
        """
        Background coroutine that wakes every DRAIN_INTERVAL seconds
        and writes all pending records to PostgreSQL.
        Never called directly — started as an asyncio Task.
        """
        while self._running:
            await asyncio.sleep(self.DRAIN_INTERVAL)
            await self._drain_once()

    async def _drain_once(self):
        """
        Pulls all pending records from the queue and writes
        them to PostgreSQL grouped by table.
        Uses executemany for each table — one round trip
        per table regardless of how many records exist.
        """
        if self._queue is None or self._queue.empty():
            return

        # Pull everything currently in the queue
        # into a local batch without waiting
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        # Group records by table
        by_table: dict = {}
        for table, record in batch:
            if table not in by_table:
                by_table[table] = []
            by_table[table].append(record)

        # Write each table's records in one executemany call
        written = 0
        failed = 0

        for table, records in by_table.items():
            query = self.INSERT_QUERIES[table]
            try:
                await database.executemany(query, records)
                written += len(records)
                logger.debug(
                    f"[WRITE BUFFER] Wrote {len(records)} " f"records to {table}."
                )
            except Exception as e:
                failed += len(records)
                logger.error(
                    f"[WRITE BUFFER] Failed to write "
                    f"{len(records)} records to {table}: {e}"
                )
                # Re-queue failed records for next drain attempt
                for record in records:
                    try:
                        self._queue.put_nowait((table, record))
                    except asyncio.QueueFull:
                        logger.error(
                            f"[WRITE BUFFER] Cannot re-queue "
                            f"failed record — queue full. "
                            f"Record lost."
                        )

        self._total_written += written
        self._total_failed += failed

        if written > 0:
            logger.debug(
                f"[WRITE BUFFER] Drain complete. "
                f"Written: {written} | "
                f"Failed: {failed} | "
                f"Queue remaining: {self._queue.qsize()}"
            )

    @property
    def queue_size(self) -> int:
        """Returns current number of pending records."""
        return self._queue.qsize() if self._queue else 0

    @property
    def is_healthy(self) -> bool:
        """
        Returns True if the write buffer is operating normally.
        Used by the health watchdog.
        """
        if not self._running:
            return False
        if self._drain_task and self._drain_task.done():
            return False
        if self.queue_size >= self.ALERT_QUEUE_SIZE:
            return False
        return True


# Single instance used across the entire system
write_buffer = WriteBuffer()
