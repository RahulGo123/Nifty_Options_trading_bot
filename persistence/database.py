import asyncpg
from decouple import config
from loguru import logger
from monitoring.alerting import send_alert

_pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        try:
            _pool = await asyncpg.create_pool(
                dsn=config("DATABASE_URL"), min_size=2, max_size=10, command_timeout=120
            )
            logger.info("[DATABASE] Connection pool created successfully.")
        except asyncpg.exceptions.CannotConnectNowError as e:
            msg = (
                f"CRITICAL: PostgreSQL unreachable at startup — {e}. "
                f"Check that PostgreSQL is running and "
                f"DATABASE_URL in .env is correct."
            )
            logger.error(f"[DATABASE] {msg}")
            await send_alert(msg)
            raise
        except Exception as e:
            msg = f"CRITICAL: Database pool creation failed — {e}"
            logger.error(f"[DATABASE] {msg}")
            await send_alert(msg)
            raise
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("[DATABASE] Connection pool closed.")


async def execute(query: str, *args) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


async def executemany(query: str, args_list: list) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(query, args_list)


async def fetch(query: str, *args) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def fetchrow(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetchval(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)


async def execute_in_transaction(queries: list) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for query, *args in queries:
                await conn.execute(query, *args)
