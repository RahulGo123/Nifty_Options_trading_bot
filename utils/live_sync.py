import json
import os
import aiohttp
from datetime import datetime
from loguru import logger


async def sync_balance_from_dhan(auth_engine, settings, is_premarket=False) -> bool:
    if not settings.live_trading:
        return True

    url = "https://api.dhan.co/v2/fundlimit"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=auth_engine.get_headers(),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    available = float(data.get("availabelBalance", 0.0))
                    utilized = float(data.get("utilizedAmount", 0.0))
                    total = available + utilized

                    state_path = "data/capital_state.json"
                    state = {}
                    if os.path.exists(state_path):
                        with open(state_path) as f:
                            state = json.load(f)

                    state["last_updated"] = datetime.now().isoformat()
                    state["final_balance"] = round(total, 2)

                    if is_premarket:
                        state["session_starting_capital"] = round(total, 2)
                        state["session_realized_pnl"] = 0.0

                    os.makedirs("data", exist_ok=True)
                    with open(state_path, "w") as f:
                        json.dump(state, f, indent=4)

                    logger.info(
                        f"[LIVE SYNC] Dhan Balance: ₹{total:,.2f} synced to disk."
                    )
                    return True
                else:
                    logger.error(
                        f"[LIVE SYNC] API Error {resp.status}: {await resp.text()}"
                    )
    except Exception as e:
        logger.error(f"[LIVE SYNC] Connection failed: {e}")
    return False
