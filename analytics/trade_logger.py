"""
analytics/trade_logger.py
=========================
Institutional Trade Analytics Database.
Appends every closed position to a highly structured Parquet dataset for
post-trade analysis and walk-forward optimization.
"""

import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime


class TradeLogger:
    def __init__(self):
        # We store this in your existing data_store directory
        self.file_path = Path("data_store/parquet/live_trading_ledger.parquet")
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def log_trade(self, position_id: str, position_obj, exit_reason: str) -> None:
        """
        Extracts the required analytics from a closed position and appends
        it to the Parquet dataset.
        """
        try:
            # 1. Safely extract analytics snapshot
            analytics = getattr(position_obj, "entry_analytics", {})
            entry_costs = getattr(position_obj, "entry_costs", {})

            # 2. Build the strict schema record
            trade_record = {
                "position_id": position_id,
                "entry_time": getattr(position_obj, "entry_timestamp", datetime.now()),
                "exit_time": datetime.now(),
                "spread_type": getattr(position_obj, "spread_type", "UNKNOWN"),
                "lots": getattr(position_obj, "lots", 0),
                "initial_net_credit": getattr(position_obj, "net_entry_premium", 0.0),
                "net_pnl": getattr(position_obj, "realized_pnl", 0.0),
                "exit_reason": exit_reason,
                # The Greeks & Volatility Snapshot
                "vix_at_entry": getattr(position_obj, "vix_at_entry", 0.0),
                "ivr_at_entry": getattr(position_obj, "ivr_at_entry", 0.0),
                "nifty_spot_at_entry": getattr(position_obj, "nifty_spot_at_entry", 0.0),
                "portfolio_delta_at_entry": getattr(position_obj, "net_delta", 0.0),
                "portfolio_theta_at_entry": getattr(position_obj, "net_theta", 0.0),
                # Liquidity Quality
                "max_bid_ask_spread_pct": getattr(
                    position_obj, "max_bid_ask_spread_pct", 0.0
                ),
            }

            # 3. Append to Parquet
            df_new = pd.DataFrame([trade_record])

            # Ensure timestamps are uniform
            df_new["entry_time"] = pd.to_datetime(df_new["entry_time"])
            df_new["exit_time"] = pd.to_datetime(df_new["exit_time"])

            if self.file_path.exists():
                df_existing = pd.read_parquet(self.file_path)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined.to_parquet(self.file_path, engine="pyarrow", index=False)
            logger.success(
                f"[TRADE LOGGER] Parquet ledger updated. Record saved for {position_id}."
            )

        except Exception as e:
            logger.error(
                f"[TRADE LOGGER] Critical failure writing to Parquet database: {e}"
            )


# Single instance
trade_logger = TradeLogger()
