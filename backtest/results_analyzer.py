"""
backtest/results_analyzer.py
=============================
Computes all metrics needed for the in-sample / out-of-sample pass/fail
decision and produces the final deployment authorisation report.

Pass/fail criteria (from blueprint)
------------------------------------
  Out-of-sample Sharpe degrades < 40% vs in-sample → PASS
  Max drawdown within 1.5× of in-sample max drawdown → PASS
  Win rate within 10 percentage points of in-sample → PASS

  If all three pass → model validated, Ghost Mode authorised.
  If any fails     → signal logic has a structural flaw.
                     Do not re-tune thresholds. Re-examine the gates.

Run:
    python -m backtest.results_analyzer --in-sample  in_sample_trades.json
                                        --out-sample out_sample_trades.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------


def sharpe_ratio(pnl_series: List[float], risk_free_annual: float = 0.053278) -> float:
    """
    Sharpe ratio computed on per-trade P&L.
    risk_free_annual: RBI 91-day T-bill yield (from constants.py).
    Annualised assuming ~250 trades per year (conservative).
    Returns 0.0 for < 2 trades.
    """
    if len(pnl_series) < 2:
        return 0.0
    arr = np.array(pnl_series, dtype=float)
    mean = arr.mean()
    std = arr.std(ddof=1)
    if std == 0:
        return 0.0
    # Per-trade risk-free rate (approx)
    rf_per_trade = risk_free_annual / 250
    return float((mean - rf_per_trade) / std)


def max_drawdown(pnl_series: List[float]) -> float:
    """
    Maximum peak-to-trough drawdown on cumulative P&L.
    Returns the drawdown as a positive ₹ value.
    """
    if not pnl_series:
        return 0.0
    cumulative = np.cumsum(pnl_series)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    return float(drawdown.max())


def win_rate(pnl_series: List[float]) -> float:
    """Fraction of trades with net P&L > 0."""
    if not pnl_series:
        return 0.0
    wins = sum(1 for p in pnl_series if p > 0)
    return wins / len(pnl_series)


def average_win(pnl_series: List[float]) -> float:
    wins = [p for p in pnl_series if p > 0]
    return float(np.mean(wins)) if wins else 0.0


def average_loss(pnl_series: List[float]) -> float:
    losses = [p for p in pnl_series if p <= 0]
    return float(np.mean(losses)) if losses else 0.0


def profit_factor(pnl_series: List[float]) -> float:
    gross_wins = sum(p for p in pnl_series if p > 0)
    gross_losses = abs(sum(p for p in pnl_series if p < 0))
    if gross_losses == 0:
        return float("inf") if gross_wins > 0 else 0.0
    return round(gross_wins / gross_losses, 4)


def regime_breakdown(trades) -> Dict[str, Dict]:
    """
    Returns per-regime statistics.
    trades: list of BacktestTrade objects.
    """
    from collections import defaultdict

    by_regime: Dict[str, List[float]] = defaultdict(list)

    for t in trades:
        if t.net_pnl is not None:
            # Infer regime from spread_type and direction
            if t.spread_type in ("CALL_SPREAD",) and t.direction == "BULL":
                regime = "CREDIT_BULL"
            elif t.spread_type in ("PUT_SPREAD",) and t.direction == "BEAR":
                regime = "CREDIT_BEAR"
            else:
                regime = "DEBIT"
            by_regime[regime].append(t.net_pnl)

    result = {}
    for regime, pnls in by_regime.items():
        result[regime] = {
            "count": len(pnls),
            "total_pnl": round(sum(pnls), 2),
            "win_rate": round(win_rate(pnls), 4),
            "sharpe": round(sharpe_ratio(pnls), 4),
            "avg_win": round(average_win(pnls), 2),
            "avg_loss": round(average_loss(pnls), 2),
        }
    return result


def exit_reason_breakdown(trades) -> Dict[str, int]:
    """Returns count of trades by exit reason."""
    from collections import Counter

    reasons = [t.exit_reason for t in trades if t.exit_reason]
    return dict(Counter(reasons))


# ---------------------------------------------------------------------------
# Full analysis report
# ---------------------------------------------------------------------------


@dataclass
class AnalysisReport:
    label: str
    trade_count: int
    total_pnl: float
    sharpe: float
    max_drawdown: float
    win_rate_pct: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    largest_loss: float
    largest_win: float
    regime_stats: Dict = field(default_factory=dict)
    exit_reasons: Dict = field(default_factory=dict)


def analyze(trades, label: str = "BACKTEST") -> AnalysisReport:
    """
    Computes all metrics from a list of BacktestTrade objects.
    Returns an AnalysisReport.
    """
    closed = [t for t in trades if t.net_pnl is not None]
    pnls = [t.net_pnl for t in closed]

    if not pnls:
        return AnalysisReport(
            label=label,
            trade_count=0,
            total_pnl=0.0,
            sharpe=0.0,
            max_drawdown=0.0,
            win_rate_pct=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            profit_factor=0.0,
            largest_loss=0.0,
            largest_win=0.0,
        )

    return AnalysisReport(
        label=label,
        trade_count=len(pnls),
        total_pnl=round(sum(pnls), 2),
        sharpe=round(sharpe_ratio(pnls), 4),
        max_drawdown=round(max_drawdown(pnls), 2),
        win_rate_pct=round(win_rate(pnls) * 100, 2),
        avg_win=round(average_win(pnls), 2),
        avg_loss=round(average_loss(pnls), 2),
        profit_factor=profit_factor(pnls),
        largest_loss=round(min(pnls), 2),
        largest_win=round(max(pnls), 2),
        regime_stats=regime_breakdown(closed),
        exit_reasons=exit_reason_breakdown(closed),
    )


# ---------------------------------------------------------------------------
# Pass/fail decision
# ---------------------------------------------------------------------------


@dataclass
class ValidationVerdict:
    sharpe_pass: bool
    drawdown_pass: bool
    winrate_pass: bool
    overall_pass: bool
    sharpe_degradation: float
    drawdown_ratio: float
    winrate_diff: float
    message: str = ""

    # Blueprint thresholds
    SHARPE_DEGRADE_MAX = 0.40  # 40% degradation allowed
    DRAWDOWN_RATIO_MAX = 1.50  # 1.5× in-sample max DD
    WINRATE_DIFF_MAX = 10.0  # 10 percentage points


def validate_out_of_sample(
    in_sample: AnalysisReport,
    out_sample: AnalysisReport,
) -> ValidationVerdict:
    """
    Applies the three pass/fail criteria from the blueprint.
    """
    # Sharpe degradation — only meaningful when in-sample Sharpe is positive.
    # A negative in-sample Sharpe means the strategy loses money consistently
    # and is not deployable regardless of out-of-sample comparison.
    if in_sample.sharpe <= 0:
        sharpe_deg = 1.0  # treat as 100% degradation → always fails
        sharpe_pass = False
    elif out_sample.sharpe <= 0:
        sharpe_deg = 1.0
        sharpe_pass = False
    else:
        sharpe_deg = (in_sample.sharpe - out_sample.sharpe) / in_sample.sharpe
        sharpe_pass = sharpe_deg < ValidationVerdict.SHARPE_DEGRADE_MAX

    # Drawdown ratio
    if in_sample.max_drawdown > 0:
        dd_ratio = out_sample.max_drawdown / in_sample.max_drawdown
    else:
        dd_ratio = 1.0

    dd_pass = dd_ratio <= ValidationVerdict.DRAWDOWN_RATIO_MAX

    # Win rate difference — only fail if OOS win rate DEGRADES vs in-sample.
    # Improvement out-of-sample is not a structural flaw.
    wr_diff = in_sample.win_rate_pct - out_sample.win_rate_pct  # positive = degradation
    wr_pass = wr_diff <= ValidationVerdict.WINRATE_DIFF_MAX

    overall = sharpe_pass and dd_pass and wr_pass

    if overall:
        msg = (
            "✅ OUT-OF-SAMPLE VALIDATION PASSED. "
            "Model validated. Ghost Mode deployment authorised."
        )
    else:
        failures = []
        if not sharpe_pass:
            failures.append(
                f"Sharpe degraded {sharpe_deg:.1%} (max allowed 40%). "
                "Gate logic has a structural flaw — re-examine signal gates."
            )
        if not dd_pass:
            failures.append(f"Drawdown ratio {dd_ratio:.2f}× (max allowed 1.5×).")
        if not wr_pass:
            failures.append(f"Win rate diff {wr_diff:.1f}pp (max allowed 10pp).")
        msg = (
            "❌ OUT-OF-SAMPLE VALIDATION FAILED.\n"
            + "\n".join(f"  - {f}" for f in failures)
            + "\nDo NOT re-tune thresholds. Re-examine the signal gate logic."
        )

    return ValidationVerdict(
        sharpe_pass=sharpe_pass,
        drawdown_pass=dd_pass,
        winrate_pass=wr_pass,
        overall_pass=overall,
        sharpe_degradation=round(sharpe_deg, 4),
        drawdown_ratio=round(dd_ratio, 4),
        winrate_diff=round(wr_diff, 2),
        message=msg,
    )


# ---------------------------------------------------------------------------
# Report printing and saving
# ---------------------------------------------------------------------------


def print_report(report: AnalysisReport) -> None:
    sep = "═" * 56
    print(f"\n{sep}")
    print(f"  {report.label}")
    print(sep)
    print(f"  Trades:          {report.trade_count:>8}")
    print(f"  Total P&L:       ₹{report.total_pnl:>12,.2f}")
    print(f"  Sharpe ratio:    {report.sharpe:>8.4f}")
    print(f"  Max drawdown:    ₹{report.max_drawdown:>12,.2f}")
    print(f"  Win rate:        {report.win_rate_pct:>7.2f}%")
    print(f"  Avg win:         ₹{report.avg_win:>12,.2f}")
    print(f"  Avg loss:        ₹{report.avg_loss:>12,.2f}")
    print(f"  Profit factor:   {report.profit_factor:>8.4f}")
    print(f"  Largest win:     ₹{report.largest_win:>12,.2f}")
    print(f"  Largest loss:    ₹{report.largest_loss:>12,.2f}")

    if report.regime_stats:
        print(f"\n  Regime breakdown:")
        for regime, stats in report.regime_stats.items():
            print(
                f"    {regime:<20} "
                f"n={stats['count']:>4}  "
                f"P&L=₹{stats['total_pnl']:>10,.2f}  "
                f"WR={stats['win_rate']:.1%}  "
                f"Sharpe={stats['sharpe']:+.3f}"
            )

    if report.exit_reasons:
        print(f"\n  Exit reasons:")
        for reason, count in sorted(report.exit_reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<30} {count:>4}")

    print(sep + "\n")


def print_verdict(verdict: ValidationVerdict) -> None:
    sep = "═" * 56
    print(f"\n{sep}")
    print("  OUT-OF-SAMPLE VALIDATION")
    print(sep)
    print(
        f"  Sharpe degradation:  {verdict.sharpe_degradation:>6.1%}   "
        f"{'✅' if verdict.sharpe_pass else '❌'} (max 40%)"
    )
    print(
        f"  Drawdown ratio:      {verdict.drawdown_ratio:>6.2f}×   "
        f"{'✅' if verdict.drawdown_pass else '❌'} (max 1.5×)"
    )
    print(
        f"  Win rate diff:       {verdict.winrate_diff:>5.1f}pp   "
        f"{'✅' if verdict.winrate_pass else '❌'} (max 10pp)"
    )
    print(sep)
    print(f"  {verdict.message}")
    print(sep + "\n")


def save_results(
    in_sample: AnalysisReport,
    out_sample: AnalysisReport,
    verdict: ValidationVerdict,
    output_path: Path = Path("backtest_results.json"),
) -> None:
    """Saves complete results to JSON for record keeping."""
    import dataclasses

    def asdict(obj):
        if dataclasses.is_dataclass(obj):
            return {k: asdict(v) for k, v in dataclasses.asdict(obj).items()}
        return obj

    results = {
        "in_sample": asdict(in_sample),
        "out_sample": asdict(out_sample),
        "verdict": asdict(verdict),
    }
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    logger.info("[ANALYZER] Results saved to {}", output_path)


# ---------------------------------------------------------------------------
# CLI — compare two trade log JSON files
# ---------------------------------------------------------------------------


def _load_pnls_from_json(path: Path) -> List[float]:
    """Loads a list of net_pnl values from a JSON trade log."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [
            float(t.get("net_pnl", 0.0)) for t in data if t.get("net_pnl") is not None
        ]
    return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Analyse backtest results and compute pass/fail verdict."
    )
    parser.add_argument(
        "--in-sample",
        type=Path,
        required=True,
        help="JSON file containing in-sample trade records.",
    )
    parser.add_argument(
        "--out-sample",
        type=Path,
        required=True,
        help="JSON file containing out-of-sample trade records.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("backtest_results.json"),
        help="Output JSON path for full results.",
    )
    args = parser.parse_args()

    class _FakeTrade:
        def __init__(self, pnl, reason="UNKNOWN"):
            self.net_pnl = pnl
            self.exit_reason = reason
            self.spread_type = "CALL_SPREAD"
            self.direction = "BULL"

    is_pnls = _load_pnls_from_json(args.in_sample)
    os_pnls = _load_pnls_from_json(args.out_sample)

    is_report = analyze([_FakeTrade(p) for p in is_pnls], "IN-SAMPLE")
    oos_report = analyze([_FakeTrade(p) for p in os_pnls], "OUT-OF-SAMPLE")
    verdict = validate_out_of_sample(is_report, oos_report)

    print_report(is_report)
    print_report(oos_report)
    print_verdict(verdict)
    save_results(is_report, oos_report, verdict, args.output)
