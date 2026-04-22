import sys
import time
from datetime import date
from pathlib import Path
import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.backtest_engine import (
    BacktestParams,
    run_episode_precomputed,
    precompute_episode,
)

# ---------------------------------------------------------------------------
# TRUE REALITY CONFIGURATION (DHAN HQ)
# ---------------------------------------------------------------------------
STARTING_CAPITAL = 100_000.0
RISK_FRACTION = 0.90  # 30% of current capital allocated to Margin
MONTHLY_FIXED_OPS = 0.0  # Dhan has Zero AMC.

START_DATE = date(2025, 1, 1)  # Match the start of your verified IS ledger
END_DATE = date(2026, 3, 28)  # Match the end of your verified OOS ledger

# THE GOLDILOCKS PARAMETERS (Locked In)
PARAMS = BacktestParams(
    risk_per_trade=30_000.0,
    min_vix=11.0,
    max_vix=22.0,  # The Tariff-Shock Shield
    vwap_pullback_tolerance=0.0045,  # The Noise Filter
    atr_multiplier=2.0,
    profit_target=1,  # 75% Theta Harvester
    spread_width=6,  # 300-point wings
    theta_days=5,
    min_days_to_expiry=0,
)
PARQUET_DIR = Path("data_store/parquet")
SMA_PATH = Path("data_store/parquet/nifty_fut_sma50.parquet")
EXCLUDED_PATH = Path("data_store/parquet/excluded_days.txt")


# ---------------------------------------------------------------------------
# DATA LOADERS (Chronological)
# ---------------------------------------------------------------------------
def load_chronological_files():
    files = []
    for fp in sorted(PARQUET_DIR.glob("weekly_*.parquet")):
        try:
            df = pd.read_parquet(fp, engine="pyarrow", columns=["expiry"])
            exp = pd.Timestamp(df["expiry"].iloc[0]).date()

            if START_DATE <= exp <= END_DATE:
                files.append((exp, fp))

        except Exception:
            pass

    # Sort strictly by time to prevent any look-ahead bias
    return sorted(files, key=lambda x: x[0])


def load_excluded_days() -> set:
    if not EXCLUDED_PATH.exists():
        return set()
    return {
        date.fromisoformat(line.strip())
        for line in EXCLUDED_PATH.read_text().splitlines()
        if line.strip()
    }


def load_sma() -> pd.Series:
    sma = pd.read_parquet(SMA_PATH)["sma50"]
    if hasattr(sma.index, "tz") and sma.index.tz is not None:
        sma.index = sma.index.tz_localize(None)
    sma.index = sma.index.normalize()
    return sma


# ---------------------------------------------------------------------------
# TRUE EVENT-DRIVEN EXECUTION
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    t_start = time.time()

    excluded_days = load_excluded_days()
    sma_series = load_sma()
    chrono_files = load_chronological_files()

    current_capital = STARTING_CAPITAL
    all_trades = []

    current_month = None
    monthly_pnl = 0
    monthly_trade_count = 0

    # 1. USE THE STATIC BACKTEST ASSUMPTION
    current_margin = PARAMS.margin_per_lot

    print("\n=== THE TRUE CHRONOLOGICAL LEDGER (DHAN) ===")
    print(f"Starting Capital: ₹{STARTING_CAPITAL:,.2f}")
    print(f"Margin Assumption: ₹{current_margin:,.2f} per Condor (Static)")
    print(f"Brokerage Model: Flat ₹20 per executed order")
    print("-" * 65)

    for expiry, fp in chrono_files:
        df = pd.read_parquet(fp, engine="pyarrow")
        exp, bars = precompute_episode(df, sma_series, excluded_days)

        if not exp or not bars:
            continue

        # 1. THE REALITY CHECK: Update risk dynamically based on REAL current capital
        desired_risk_rupees = current_capital * RISK_FRACTION

        # 2. THE MARGIN CAP: Calculate using static backtest margin
        usable_capital = current_capital * RISK_FRACTION
        max_allowed_lots = int(usable_capital / PARAMS.margin_per_lot)

        if max_allowed_lots < 1:
            logger.warning(
                f"Insufficient capital (₹{current_capital:,.2f}) to trade expiry {expiry}"
            )
            continue

        # 3. FEED THE ENGINE: Pass the actual reality metrics down to the tick simulator
        PARAMS.risk_per_trade = desired_risk_rupees
        PARAMS.max_lots_override = max_allowed_lots

        # Run the single week
        weekly_trades = run_episode_precomputed(exp, bars, PARAMS)

        for t in weekly_trades:
            if t.net_pnl is None:
                continue

            trade_month = t.exit_time.strftime("%Y-%m")

            if current_month is None:
                current_month = trade_month

            if trade_month != current_month:
                current_capital -= MONTHLY_FIXED_OPS
                print(
                    f"{current_month} | Trades: {monthly_trade_count:02d} | P&L: ₹{monthly_pnl:8,.0f} | End Balance: ₹{current_capital:10,.0f}"
                )

                current_month = trade_month
                monthly_pnl = 0
                monthly_trade_count = 0

            monthly_pnl += t.net_pnl
            current_capital += t.net_pnl
            monthly_trade_count += 1
            all_trades.append(t)

            # --- Individual Trade Log ---
            entry_str = t.entry_time.strftime("%Y-%m-%d %H:%M")
            exit_str = t.exit_time.strftime("%Y-%m-%d %H:%M") if t.exit_time else "OPEN"
            pnl_color = "+" if t.net_pnl >= 0 else ""
            print(
                f"  > [{entry_str}] -> [{exit_str}] | {t.spread_type:<12} | P&L: {pnl_color}₹{t.net_pnl:8,.2f}"
            )

            if current_capital <= 0:
                print(f"\n[!!!] ACCOUNT BLOWN UP ON {t.exit_time}")
                sys.exit(1)

    # Print the final month
    if current_month:
        current_capital -= MONTHLY_FIXED_OPS
        print(
            f"{current_month} | Trades: {monthly_trade_count:02d} | P&L: ₹{monthly_pnl:8,.0f} | End Balance: ₹{current_capital:10,.0f}"
        )

    print("-" * 65)
    print(f"Final Net Capital: ₹{current_capital:,.2f}")

    if len(all_trades) > 0:
        win_rate = sum(1 for t in all_trades if t.net_pnl > 0) / len(all_trades)
        print(f"True Compounded Win Rate: {win_rate:.1%}")
    else:
        print("True Compounded Win Rate: 0.0% (No trades executed)")

    print(f"Total time: {(time.time() - t_start):.1f} seconds")

    # ---------------------------------------------------------------------------
    # JSON EXPORT
    # ---------------------------------------------------------------------------
    import json

    def trade_to_dict(t):
        return {
            "trading_date": str(t.trading_date),
            "entry_time": t.entry_time.isoformat(),
            "exit_time": t.exit_time.isoformat() if t.exit_time else None,
            "direction": t.direction,
            "spread_type": t.spread_type,
            "lots": t.lots,
            "capital_invested": float(t.lots * PARAMS.margin_per_lot),
            "entry_vix": t.entry_vix,
            "net_pnl": t.net_pnl,
            "exit_reason": t.exit_reason,
            "entry_long": t.entry_long,
            "entry_short": t.entry_short,
            "exit_long": t.exit_long,
            "exit_short": t.exit_short,
            "entry_costs": t.entry_costs,
            "exit_costs": t.exit_costs,
        }

    closed_trades = [t for t in all_trades if t.net_pnl is not None]
    export_path = Path("trades.json")

    export_path.write_text(
        json.dumps([trade_to_dict(t) for t in closed_trades], indent=2)
    )
    print(f"Exported {len(closed_trades)} completed trades to {export_path.name}")
