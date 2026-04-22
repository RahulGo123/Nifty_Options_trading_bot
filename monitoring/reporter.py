"""
monitoring/reporter.py
======================
End-of-day P&L report builder.

Called once at 03:10 PM by the hard kill sequence in
positions/lifecycle_manager.py (or main.py).

The report is assembled entirely from the in-memory PortfolioState
plus the closed_trades list — it does NOT query PostgreSQL.
The database write buffer may still be draining at 03:10 PM;
reading from memory ensures the report is always consistent and instant.

Report sections
---------------
1. Session header — date, mode, starting capital, IVR regime distribution
2. P&L summary — realized, unrealized (should be zero at hard kill), net
3. Trade breakdown — count, win rate, avg win, avg loss, largest single loss
4. Cost summary — total STT, NSE charges, brokerage, GST paid today
5. Risk events — circuit breaker trips, stop-losses, theta exits, rolls
6. Regime log — minutes spent in Credit / Debit / Standby
7. Execution quality — avg slippage per leg vs synthetic model
8. Footer — next session reminders (rate verification staleness)
"""

from __future__ import annotations

import logging
import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional

from monitoring.alerting import send_raw

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures — populated by lifecycle_manager and passed to build_report()
# ---------------------------------------------------------------------------


@dataclass
class ClosedTrade:
    """Mirrors the subset of fields needed by the reporter."""

    entry_time: datetime
    exit_time: datetime
    direction: str  # "BULL" | "BEAR"
    strategy: str  # "CALL_SPREAD" | "PUT_SPREAD" etc.
    strikes: str  # human-readable e.g. "22500CE/22600CE"
    lots: int
    gross_pnl: float  # before costs
    entry_costs: float
    exit_costs: float
    exit_reason: (
        str  # "STOP_LOSS" | "THETA_EXIT" | "HARD_KILL" | "NEAR_EXPIRY" | "MANUAL_ROLL"
    )
    entry_premium: float
    exit_premium: float
    entry_slippage: float  # actual slippage paid (Ask - mid at entry)
    exit_slippage: float  # actual slippage paid (mid - Bid at exit)

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.entry_costs - self.exit_costs

    @property
    def is_winner(self) -> bool:
        return self.net_pnl > 0


@dataclass
class RegimeLog:
    """Accumulated minutes spent in each IVR regime during the session."""

    credit_minutes: int = 0
    debit_minutes: int = 0
    standby_minutes: int = 0

    @property
    def total_minutes(self) -> int:
        return self.credit_minutes + self.debit_minutes + self.standby_minutes


@dataclass
class CarryPosition:
    """Represents an open position being carried overnight."""

    strategy: str
    strikes: str
    lots: int
    unrealized_pnl: float


@dataclass
class SessionSummary:
    """
    Everything needed to build the EOD report.
    Populated by lifecycle_manager._hard_kill() and passed to build_report().
    """

    session_date: date
    starting_capital: float
    live_trading: bool

    closed_trades: List[ClosedTrade] = field(default_factory=list)
    carry_over_positions: List[CarryPosition] = field(default_factory=list)
    total_unrealized_pnl: float = 0.0
    regime_log: RegimeLog = field(default_factory=RegimeLog)
    new_trades_count: int = 0

    # Risk events
    circuit_breaker_tripped: bool = False
    circuit_breaker_pnl: Optional[float] = None  # P&L at trip time
    stop_loss_count: int = 0
    theta_exit_count: int = 0
    near_expiry_exit_count: int = 0
    roll_count: int = 0
    roll_failed_count: int = 0  # Rolls attempted but net credit unavailable
    signals_quarantined: int = 0  # Signals generated outside execution windows
    signals_suppressed_macro: int = 0  # Bearish signals killed by macro override

    # Execution quality
    synthetic_slippage_per_leg: float = 0.0  # From backtest cost model (for comparison)

    # Rate staleness (from rate_validator.py — pass through for footer)
    rate_stale_warnings: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


def build_report(s: SessionSummary) -> str:
    """
    Assemble the full EOD Telegram message from SessionSummary.
    Returns a single UTF-8 string with HTML formatting.
    Caller passes this to send_raw().
    """
    lines: List[str] = []
    mode = "🔴 LIVE" if s.live_trading else "👻 GHOST"

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------
    lines += [
        f"📊 <b>EOD REPORT — {mode}</b>",
        f"📅 {s.session_date.strftime('%A, %d %B %Y')}",
        "",
    ]

    # ------------------------------------------------------------------
    # P&L Summary
    # ------------------------------------------------------------------
    realized = sum(t.net_pnl for t in s.closed_trades)
    total_costs = sum(t.entry_costs + t.exit_costs for t in s.closed_trades)
    gross_realized = sum(t.gross_pnl for t in s.closed_trades)
    net_pct = (realized / s.starting_capital * 100) if s.starting_capital else 0.0

    pnl_emoji = "✅" if realized >= 0 else "❌"
    lines += [
        "<b>── P&amp;L Summary ──</b>",
        f"{pnl_emoji} Net Realized:     <code>₹{realized:>10,.2f}</code>  "
        f"({net_pct:+.2f}%)",
        f"   Floating P&amp;L:    <code>₹{s.total_unrealized_pnl:>10,.2f}</code>",
        f"   Gross Realized:   <code>₹{gross_realized:>10,.2f}</code>",
        f"   Total costs:    <code>₹{total_costs:>10,.2f}</code>",
        f"   Starting cap:   <code>₹{s.starting_capital:>10,.2f}</code>",
        "",
    ]

    # ------------------------------------------------------------------
    # Overnight Carry
    # ------------------------------------------------------------------
    if s.carry_over_positions:
        lines.append("<b>── Overnight Carry ──</b>")
        for cp in s.carry_over_positions:
            pnl_flag = "📈" if cp.unrealized_pnl >= 0 else "📉"
            lines.append(f"   {pnl_flag} [{cp.strategy}] {cp.strikes} ({cp.lots} lot) <code>₹{cp.unrealized_pnl:,.2f}</code>")
        lines.append("")

    # ------------------------------------------------------------------
    # Trade Statistics
    # ------------------------------------------------------------------
    n = len(s.closed_trades)
    lines += ["<b>── Trade Statistics ──</b>"]
    
    if s.new_trades_count > 0:
        lines.append(f"   New entries:          <code>{s.new_trades_count:>3}</code>")
        
    if n == 0:
        if s.new_trades_count == 0:
            lines.append("   No trade activity this session.")
        else:
            lines.append("   No positions closed today.")
        lines.append("")
    else:
        winners = [t for t in s.closed_trades if t.is_winner]
        losers = [t for t in s.closed_trades if not t.is_winner]
        win_rate = len(winners) / n * 100

        avg_win = (sum(t.net_pnl for t in winners) / len(winners)) if winners else 0.0
        avg_loss = (sum(t.net_pnl for t in losers) / len(losers)) if losers else 0.0
        largest_loss = min((t.net_pnl for t in s.closed_trades), default=0.0)
        largest_win = max((t.net_pnl for t in s.closed_trades), default=0.0)

        lines += [
            "<b>── Trade Statistics ──</b>",
            f"   Total trades:   <code>{n:>4}</code>",
            f"   Winners:        <code>{len(winners):>4}</code>  " f"({win_rate:.1f}%)",
            f"   Losers:         <code>{len(losers):>4}</code>",
            f"   Avg win:        <code>₹{avg_win:>8,.2f}</code>",
            f"   Avg loss:       <code>₹{avg_loss:>8,.2f}</code>",
            f"   Largest win:    <code>₹{largest_win:>8,.2f}</code>",
            f"   Largest loss:   <code>₹{largest_loss:>8,.2f}</code>",
            "",
        ]

        # Direction breakdown
        bull_trades = [t for t in s.closed_trades if t.direction == "BULL"]
        bear_trades = [t for t in s.closed_trades if t.direction == "BEAR"]
        if bull_trades or bear_trades:
            lines += [
                "<b>── Direction Breakdown ──</b>",
                f"   Bullish trades: <code>{len(bull_trades):>4}</code>  "
                f"P&amp;L <code>₹{sum(t.net_pnl for t in bull_trades):,.2f}</code>",
                f"   Bearish trades: <code>{len(bear_trades):>4}</code>  "
                f"P&amp;L <code>₹{sum(t.net_pnl for t in bear_trades):,.2f}</code>",
                "",
            ]

    # ------------------------------------------------------------------
    # Cost Breakdown
    # ------------------------------------------------------------------
    if s.closed_trades:
        # Costs are stored as total per trade — we can show aggregate only
        # (per-component breakdown requires cost_model to return itemised dict,
        # which is a future enhancement).
        lines += [
            "<b>── Cost Summary ──</b>",
            f"   Total transaction costs: <code>₹{total_costs:,.2f}</code>",
            f"   Cost per trade (avg):    " f"<code>₹{total_costs / n:,.2f}</code>",
            "",
        ]

    # ------------------------------------------------------------------
    # Exit Reasons
    # ------------------------------------------------------------------
    if s.closed_trades:
        reason_counts: Dict[str, int] = {}
        for t in s.closed_trades:
            reason_counts[t.exit_reason] = reason_counts.get(t.exit_reason, 0) + 1

        lines.append("<b>── Exit Reasons ──</b>")
        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            lines.append(f"   {reason:<22} <code>{count:>3}</code>")
        lines.append("")

    # ------------------------------------------------------------------
    # Risk Events
    # ------------------------------------------------------------------
    lines.append("<b>── Risk Events ──</b>")
    if s.circuit_breaker_tripped:
        cb_pnl_str = (
            f"₹{s.circuit_breaker_pnl:,.2f}"
            if s.circuit_breaker_pnl is not None
            else "N/A"
        )
        lines.append(f"   🚨 CIRCUIT BREAKER TRIPPED at P&amp;L {cb_pnl_str}")
    else:
        lines.append("   ✅ Circuit breaker: not triggered")

    lines += [
        f"   Stop-losses triggered:  <code>{s.stop_loss_count:>3}</code>",
        f"   Theta stagnation exits: <code>{s.theta_exit_count:>3}</code>",
        f"   Near-expiry exits:      <code>{s.near_expiry_exit_count:>3}</code>",
        f"   Rolls executed:         <code>{s.roll_count:>3}</code>",
        f"   Rolls failed (closed):  <code>{s.roll_failed_count:>3}</code>",
        f"   Signals quarantined:    <code>{s.signals_quarantined:>3}</code>",
        f"   Signals macro-suppressed: <code>{s.signals_suppressed_macro:>2}</code>",
        "",
    ]

    # ------------------------------------------------------------------
    # IVR Regime Distribution
    # ------------------------------------------------------------------
    rl = s.regime_log
    total_min = rl.total_minutes or 1  # avoid div-by-zero
    lines += [
        "<b>── IVR Regime Distribution ──</b>",
        f"   Credit:  {_bar(rl.credit_minutes, total_min)}  "
        f"<code>{rl.credit_minutes:>3} min</code>  "
        f"({rl.credit_minutes / total_min * 100:.0f}%)",
        f"   Debit:   {_bar(rl.debit_minutes,  total_min)}  "
        f"<code>{rl.debit_minutes:>3} min</code>  "
        f"({rl.debit_minutes / total_min * 100:.0f}%)",
        f"   Standby: {_bar(rl.standby_minutes, total_min)}  "
        f"<code>{rl.standby_minutes:>3} min</code>  "
        f"({rl.standby_minutes / total_min * 100:.0f}%)",
        "",
    ]

    # ------------------------------------------------------------------
    # Execution Quality
    # ------------------------------------------------------------------
    if s.closed_trades:
        total_entry_slip = sum(t.entry_slippage for t in s.closed_trades)
        total_exit_slip = sum(t.exit_slippage for t in s.closed_trades)
        total_slip = total_entry_slip + total_exit_slip
        legs = n * 4  # 2 legs entry + 2 legs exit per spread
        avg_slip_per_leg = total_slip / legs if legs else 0.0

        lines += [
            "<b>── Execution Quality ──</b>",
            f"   Total slippage paid:  <code>₹{total_slip:,.2f}</code>",
            f"   Avg slippage per leg: <code>₹{avg_slip_per_leg:.2f}</code>",
            f"   Synthetic model est:  <code>₹{s.synthetic_slippage_per_leg:.2f}</code>",
        ]
        diff = avg_slip_per_leg - s.synthetic_slippage_per_leg
        flag = "⚠️ " if diff > 1.0 else "✅ "
        lines += [
            f"   {flag}Actual vs model:    <code>₹{diff:+.2f}</code> per leg",
            "",
        ]

    # ------------------------------------------------------------------
    # Tomorrow's Macro Events (T+1)
    # ------------------------------------------------------------------
    try:
        from macro.event_calendar import event_calendar
        from datetime import timedelta

        tomorrow = s.session_date + timedelta(days=1)
        # Fetch events for the next day
        tomorrow_events = event_calendar.get_events_for_date(tomorrow)

        lines.append("<b>── Tomorrow's Macro Events (T+1) ──</b>")
        if tomorrow_events:
            for e in tomorrow_events:
                # Assuming 'time' and 'name' are attributes of your event object
                time_str = (
                    e.time.strftime("%H:%M")
                    if hasattr(e.time, "strftime")
                    else str(e.time)
                )
                lines.append(f"   • <code>{time_str}</code> - {e.name}")
        else:
            lines.append("   No Tier-1 events scheduled.")
        lines.append("")
    except Exception as e:
        logger.debug(f"[REPORTER] Could not fetch tomorrow's events for report: {e}")

    # ------------------------------------------------------------------
    # Footer — rate staleness reminders
    # ------------------------------------------------------------------
    if s.rate_stale_warnings:
        lines.append("<b>── Action Required ──</b>")
        for warning in s.rate_stale_warnings:
            lines.append(f"   ⚠️ {warning}")
        lines.append("")

    lines.append(
        f"<i>Report generated at {datetime.now().strftime('%H:%M:%S')} IST</i>"
    )

    return "\n".join(lines)


def _bar(value: int, total: int, width: int = 8) -> str:
    """ASCII progress bar for regime distribution."""
    filled = round(value / total * width) if total else 0
    return "█" * filled + "░" * (width - filled)


# ---------------------------------------------------------------------------
# Main dispatch function — called by lifecycle_manager._hard_kill()
# ---------------------------------------------------------------------------


def dispatch_eod_report(summary: SessionSummary) -> None:
    """
    Build and enqueue the EOD report via alerting.send_raw().

    This is the only function lifecycle_manager needs to call:

        from monitoring.reporter import dispatch_eod_report
        dispatch_eod_report(session_summary)

    The report is fire-and-forget — it joins the Telegram alert queue
    and will be sent even if the market session has formally ended.
    """
    try:
        realized = sum(t.net_pnl for t in summary.closed_trades)
        final_balance = summary.starting_capital + realized
        
        # Save to persistent state file
        os.makedirs("data", exist_ok=True)
        state_path = os.path.join("data", "capital_state.json")
        with open(state_path, "w") as f:
            json.dump({
                "last_updated": datetime.now().isoformat(),
                "final_balance": round(final_balance, 2)
            }, f, indent=4)
        
        logger.info(f"[REPORTER] Capital state saved: ₹{final_balance:,.2f}")
        
        report_text = build_report(summary)
        send_raw(report_text)
        logger.info("EOD report enqueued for Telegram dispatch.")
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to build or enqueue EOD report: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# CLI entry point — for manual testing with dummy data
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from datetime import date, datetime, timedelta

    _now = datetime.now()

    dummy_trades = [
        ClosedTrade(
            entry_time=_now - timedelta(hours=3),
            exit_time=_now - timedelta(hours=2),
            direction="BULL",
            strategy="CALL_SPREAD",
            strikes="22500CE/22600CE",
            lots=2,
            gross_pnl=1200.0,
            entry_costs=180.0,
            exit_costs=180.0,
            exit_reason="STOP_LOSS",
            entry_premium=85.0,
            exit_premium=55.0,
            entry_slippage=4.50,
            exit_slippage=3.75,
        ),
        ClosedTrade(
            entry_time=_now - timedelta(hours=2),
            exit_time=_now - timedelta(hours=1),
            direction="BEAR",
            strategy="PUT_SPREAD",
            strikes="22300PE/22200PE",
            lots=1,
            gross_pnl=-800.0,
            entry_costs=95.0,
            exit_costs=95.0,
            exit_reason="THETA_EXIT",
            entry_premium=70.0,
            exit_premium=110.0,
            entry_slippage=3.20,
            exit_slippage=2.80,
        ),
        ClosedTrade(
            entry_time=_now - timedelta(hours=1),
            exit_time=_now - timedelta(minutes=10),
            direction="BULL",
            strategy="CALL_SPREAD",
            strikes="22550CE/22650CE",
            lots=3,
            gross_pnl=2400.0,
            entry_costs=270.0,
            exit_costs=270.0,
            exit_reason="HARD_KILL",
            entry_premium=90.0,
            exit_premium=50.0,
            entry_slippage=6.30,
            exit_slippage=5.10,
        ),
    ]

    dummy_summary = SessionSummary(
        session_date=date.today(),
        starting_capital=500_000.0,
        live_trading=False,
        closed_trades=dummy_trades,
        regime_log=RegimeLog(credit_minutes=180, debit_minutes=30, standby_minutes=40),
        circuit_breaker_tripped=False,
        stop_loss_count=1,
        theta_exit_count=1,
        near_expiry_exit_count=0,
        roll_count=0,
        roll_failed_count=0,
        signals_quarantined=2,
        signals_suppressed_macro=1,
        synthetic_slippage_per_leg=2.50,
        rate_stale_warnings=[
            "NSE transaction rate — verify at nseindia.com/regulations/circulars"
        ],
    )

    report = build_report(dummy_summary)
    print(report)
    print(f"\n[{len(report)} characters]")
