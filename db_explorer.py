import asyncio
from persistence.database import fetch, fetchval
from loguru import logger

async def explore():
    tables = ["iv_history", "runtime_config", "trades", "positions", "sessions"]
    logger.info("🔍 --- Nifty Bot Database Explorer ---")

    for table in tables:
        try:
            count = await fetchval(f"SELECT count(*) FROM {table};")
            # Get the most recent entry based on typical ID or Date columns
            order_col = "date" if table in ["iv_history", "sessions"] else "id"
            last_row = await fetch(f"SELECT * FROM {table} ORDER BY {order_col} DESC LIMIT 1;")

            logger.info(f"📁 Table: {table} | Total Rows: {count}")
            if last_row:
                logger.info(f"   └─ Latest: {dict(last_row[0])}")
            else:
                logger.info("   └─ Table is currently empty.")
        except Exception as e:
            logger.error(f"❌ Error reading {table}: {e}")

if __name__ == "__main__":
    asyncio.run(explore())
