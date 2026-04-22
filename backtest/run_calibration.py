from __future__ import annotations

import multiprocessing
import itertools
import sys
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from backtest.backtest_engine import (
    BacktestParams,
    BacktestTrade,
    run_episode,
    precompute_episode,
    run_episode_precomputed,
)
from backtest.results_analyzer import sharpe_ratio

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N_WORKERS = max(1, multiprocessing.cpu_count() - 1)
PARQUET_DIR = Path("data_store/parquet")
SMA_PATH = Path("data_store/parquet/nifty_fut_sma50.parquet")
EXCLUDED_PATH = Path("data_store/parquet/excluded_days.txt")
MIN_TRADES = 5

_EXPIRY_FILE_MAP: Optional[Dict[date, Path]] = None

# ---------------------------------------------------------------------------
# THE GOLDILOCKS GRID (30% Risk / High Premium Extraction)
# ---------------------------------------------------------------------------

WALK_FORWARD_WINDOWS = [
    (date(2022, 1, 1), date(2022, 12, 31), date(2023, 1, 1), date(2023, 6, 30)),
    (date(2022, 7, 1), date(2023, 6, 30), date(2023, 7, 1), date(2023, 12, 31)),
    (date(2023, 1, 1), date(2023, 12, 31), date(2024, 1, 1), date(2024, 6, 30)),
    (date(2023, 7, 1), date(2024, 6, 30), date(2024, 7, 1), date(2024, 12, 31)),
]

OUT_OF_SAMPLE = (date(2025, 1, 1), date(2026, 3, 28))

PARAM_GRID = {
    "risk_per_trade": [33333.0],  # 30% Base Risk Allocation
    "min_vix": [11.0, 11.5],  # Unlock slightly more trade volume
    "vwap_pullback_tolerance": [0.002, 0.003],
    "atr_multiplier": [2.0, 2.5],  # Balanced stop-losses
    "profit_target": [0.65, 0.75],  # Re-engage aggressive profit extraction
    "spread_width": [4],  # Lock in 200-point wings
    "theta_days": [2],
    "min_days_to_expiry": [0],
}

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------


def _build_expiry_map() -> Dict[date, Path]:
    global _EXPIRY_FILE_MAP
    if _EXPIRY_FILE_MAP is not None:
        return _EXPIRY_FILE_MAP
    m: Dict[date, Path] = {}
    for fp in sorted(PARQUET_DIR.glob("weekly_*.parquet")):
        try:
            df_h = pd.read_parquet(fp, engine="pyarrow", columns=["expiry"])
            exp = pd.Timestamp(df_h["expiry"].iloc[0]).date()
            m[exp] = fp
        except Exception:
            pass
    _EXPIRY_FILE_MAP = m
    return m


def files_in_window(start: date, end: date) -> List[Path]:
    return [fp for exp, fp in _build_expiry_map().items() if start <= exp <= end]


def load_excluded_days() -> set:
    if not EXCLUDED_PATH.exists():
        return set()
    result = set()
    for line in EXCLUDED_PATH.read_text(encoding="utf-8").strip().splitlines():
        try:
            result.add(date.fromisoformat(line.strip()))
        except ValueError:
            pass
    return result


def load_sma() -> pd.Series:
    if not SMA_PATH.exists():
        logger.error("[CALIBRATION] 50-day SMA file missing!")
        return pd.Series(dtype=float)

    sma = pd.read_parquet(SMA_PATH)["sma50"]
    if isinstance(sma.index, pd.DatetimeIndex) and sma.index.tz is not None:
        sma.index = sma.index.tz_localize(None)

    sma.index = sma.index.normalize()
    return sma


# ---------------------------------------------------------------------------
# Parallel Workers & Helpers
# ---------------------------------------------------------------------------

_worker_episodes = None


def _init_worker(episodes):
    global _worker_episodes
    _worker_episodes = episodes


def _prep_file(args: tuple) -> Optional[Tuple]:
    fp, sma_series, excluded_days = args
    try:
        df = pd.read_parquet(fp, engine="pyarrow")
        exp, bars = precompute_episode(df, sma_series, excluded_days)
        if exp and bars:
            return (exp, bars)
    except Exception:
        pass
    return None


def _evaluate_combo(args: tuple) -> tuple:
    combo, keys = args
    d = dict(zip(keys, combo))
    p = BacktestParams(**d)

    trades = []
    for expiry, bars in _worker_episodes:
        try:
            trades.extend(run_episode_precomputed(expiry, bars, p))
        except Exception:
            pass

    closed = [t for t in trades if t.net_pnl is not None]
    if len(closed) < MIN_TRADES:
        return (float("-inf"), d)

    sr = sharpe_ratio([t.net_pnl for t in closed])
    return (sr, d)


def _run_val_episode(args: tuple) -> List[BacktestTrade]:
    fp, p, sma, excl = args
    try:
        return run_episode(fp, p, sma, excl)
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Walk-forward Logic
# ---------------------------------------------------------------------------


def run_walk_forward_parallel(
    sma_series: pd.Series, excluded_days: set
) -> Tuple[BacktestParams, List[Dict]]:
    keys = list(PARAM_GRID.keys())
    combos = list(itertools.product(*[PARAM_GRID[k] for k in keys]))

    logger.info(
        "[CALIBRATION] GOLDILOCKS GRID: {} combos × {} workers", len(combos), N_WORKERS
    )

    window_results: List[Dict] = []
    best_avg_sharpe = float("-inf")
    best_params = BacktestParams()

    for w_idx, (tr_s, tr_e, va_s, va_e) in enumerate(WALK_FORWARD_WINDOWS):
        train_files = files_in_window(tr_s, tr_e)
        val_files = files_in_window(va_s, va_e)

        if not train_files or not val_files:
            continue

        train_pre = []
        prep_args = [(fp, sma_series, excluded_days) for fp in train_files]
        with multiprocessing.Pool(processes=N_WORKERS) as prep_pool:
            for res in prep_pool.imap_unordered(_prep_file, prep_args, chunksize=1):
                if res:
                    train_pre.append(res)

        if not train_pre:
            continue

        results = []
        with multiprocessing.Pool(
            processes=N_WORKERS, initializer=_init_worker, initargs=(train_pre,)
        ) as eval_pool:
            for i, res in enumerate(
                eval_pool.imap_unordered(
                    _evaluate_combo, [(c, keys) for c in combos], chunksize=1
                )
            ):
                results.append(res)

        results.sort(key=lambda x: x[0], reverse=True)
        best_train_sharpe, best_d = results[0]

        if best_train_sharpe == float("-inf"):
            logger.warning(
                f"Window {w_idx+1} dropped. No parameter combinations achieved {MIN_TRADES} trades."
            )
            continue

        best_window_params = BacktestParams(**best_d)

        val_trades = []
        val_args = [
            (fp, best_window_params, sma_series, excluded_days) for fp in val_files
        ]
        with multiprocessing.Pool(processes=N_WORKERS) as val_pool:
            for res in val_pool.imap_unordered(_run_val_episode, val_args, chunksize=1):
                if res:
                    val_trades.extend(res)

        closed_val = [t for t in val_trades if t.net_pnl is not None]
        val_sharpe = (
            sharpe_ratio([t.net_pnl for t in closed_val])
            if len(closed_val) >= MIN_TRADES
            else 0.0
        )
        val_pnl = sum(t.net_pnl for t in closed_val)

        logger.info(
            f"Window {w_idx+1} Val: Sharpe={val_sharpe:.4f} | Trades={len(closed_val)} | P&L=₹{val_pnl:,.0f}"
        )

        window_results.append(
            {
                "window": w_idx + 1,
                "val_sharpe": val_sharpe,
                "val_trades": len(closed_val),
                "val_pnl": val_pnl,
                "best_params": asdict(best_window_params),
            }
        )

        if val_sharpe > best_avg_sharpe:
            best_avg_sharpe = val_sharpe
            best_params = best_window_params

    return best_params, window_results


def run_oos(params, sma_series, excluded_days):
    oos_start, oos_end = OUT_OF_SAMPLE
    oos_files = files_in_window(oos_start, oos_end)
    if not oos_files:
        return []

    trades = []
    oos_args = [(fp, params, sma_series, excluded_days) for fp in oos_files]
    with multiprocessing.Pool(processes=N_WORKERS) as oos_pool:
        for res in oos_pool.imap_unordered(_run_val_episode, oos_args, chunksize=1):
            if res:
                trades.extend(res)

    return trades


# ---------------------------------------------------------------------------
# Execution & Geometric Compounding Ledger
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    multiprocessing.freeze_support()
    t_start = time.time()

    excluded_days = load_excluded_days()
    sma_series = load_sma()

    best_params, window_results = run_walk_forward_parallel(sma_series, excluded_days)

    if not window_results:
        logger.error("CRITICAL: All walk-forward windows failed.")
        sys.exit(1)

    is_trades: List[BacktestTrade] = []
    is_args = []
    for wr in window_results:
        va_s = date.fromisoformat(WALK_FORWARD_WINDOWS[wr["window"] - 1][2].isoformat())
        va_e = date.fromisoformat(WALK_FORWARD_WINDOWS[wr["window"] - 1][3].isoformat())
        p = BacktestParams(**wr["best_params"])
        for fp in files_in_window(va_s, va_e):
            is_args.append((fp, p, sma_series, excluded_days))

    if is_args:
        with multiprocessing.Pool(processes=N_WORKERS) as is_pool:
            for res in is_pool.imap_unordered(_run_val_episode, is_args, chunksize=1):
                if res:
                    is_trades.extend(res)

    oos_trades = run_oos(best_params, sma_series, excluded_days)

    # ---------------------------------------------------------------------------
    # TRUE FINANCIAL LEDGER SIMULATION (UNCHAINED COMPOUNDING)
    # ---------------------------------------------------------------------------
    STARTING_CAPITAL = 100_000.0
    MONTHLY_FIXED_OPS = 153.40

    def simulate_ledger(trades: List[BacktestTrade], start_capital: float, label: str):
        if not trades:
            return start_capital

        trades.sort(key=lambda t: t.exit_time)
        current_capital = start_capital

        monthly_ledger = {}
        for t in trades:
            if t.net_pnl is None:
                continue
            month_key = t.exit_time.strftime("%Y-%m")
            if month_key not in monthly_ledger:
                monthly_ledger[month_key] = []
            monthly_ledger[month_key].append(t)

        print(f"\n  === {label} MONTHLY LEDGER (UNCHAINED COMPOUNDING) ===")
        print(f"  Starting Capital: ₹{start_capital:,.2f}")

        total_gross_pnl = 0

        for month in sorted(monthly_ledger.keys()):
            monthly_trades = monthly_ledger[month]
            monthly_trade_pnl = 0

            for t in monthly_trades:
                # 1. Aligned Broker Margin Ceiling (₹65,000 per Condor)
                max_wing_lots_by_margin = int((current_capital * 0.90) / 65_000.0)

                # 2. Geometrical Risk Scaling (Scaling the 30% base risk to current capital)
                desired_wing_lots = int(t.lots * (current_capital / STARTING_CAPITAL))

                # 3. Execution Constraint (Removed the lot ceiling)
                actual_wing_lots = max(
                    1, min(desired_wing_lots, max_wing_lots_by_margin)
                )

                # 4. P&L Compounding Multiplier
                lot_multiplier = actual_wing_lots / t.lots
                compounded_pnl = t.net_pnl * lot_multiplier

                monthly_trade_pnl += compounded_pnl
                current_capital += compounded_pnl

            current_capital -= MONTHLY_FIXED_OPS
            total_gross_pnl += monthly_trade_pnl

            print(
                f"  {month} | Trades: {len(monthly_trades):02d} | Trading P&L: ₹{monthly_trade_pnl:8,.0f} | Fixed Ops: ₹{-MONTHLY_FIXED_OPS:6,.2f} | End Balance: ₹{current_capital:10,.0f}"
            )

            if current_capital <= 0:
                print(f"  [!] ACCOUNT BLOWN UP IN {month}")
                break

        print(f"  ------------------------------------------------")
        print(f"  Total Trading P&L : ₹{total_gross_pnl:,.2f}")
        print(f"  Final Net Capital : ₹{current_capital:,.2f}\n")

        return current_capital

    print(f"\n  CALIBRATION COMPLETE")
    print(f"  Best params: {best_params.label()}")

    is_closed = [t for t in is_trades if t.net_pnl is not None]
    oos_closed = [t for t in oos_trades if t.net_pnl is not None]

    simulate_ledger(is_closed, STARTING_CAPITAL, "IN-SAMPLE (2022-2024)")
    simulate_ledger(
        oos_closed, STARTING_CAPITAL, "OUT-OF-SAMPLE (2025-2026) [Hard Reset]"
    )

    logger.info(f"Total time: {(time.time() - t_start) / 60:.1f} min")

    import json

    def trade_to_dict(t):
        return {
            "trading_date": str(t.trading_date),
            "direction": t.direction,
            "spread_type": t.spread_type,
            "lots": t.lots,
            "net_pnl": t.net_pnl,
            "exit_reason": t.exit_reason,
            "entry_long": t.entry_long,
            "entry_short": t.entry_short,
        }

    Path("backtest_insample_trades.json").write_text(
        json.dumps([trade_to_dict(t) for t in is_closed], indent=2)
    )
    Path("backtest_oos_trades.json").write_text(
        json.dumps([trade_to_dict(t) for t in oos_closed], indent=2)
    )

    if is_closed:
        print(
            f"  IS Win rate: {sum(1 for t in is_closed if t.net_pnl > 0)/max(1, len(is_closed)):.1%}"
        )
    if oos_closed:
        print(
            f"  OOS Win rate: {sum(1 for t in oos_closed if t.net_pnl > 0)/max(1,len(oos_closed)):.1%}"
        )
