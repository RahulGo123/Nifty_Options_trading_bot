import os
import json
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, date as date_type
from typing import Optional
from loguru import logger
from config.constants import MAX_CONCURRENT_POSITIONS
from monitoring.alerting import send_alert
from data.runtime_config import runtime_config


@dataclass
class Position:
    position_id: str
    symbol: str
    spread_type: str
    long_strike: int
    short_strike: int
    option_type: str
    lots: int
    lot_size: int
    entry_timestamp: datetime
    expiry_date: date_type
    entry_premium_long: float
    entry_premium_short: float
    current_premium_long: float = 0.0
    current_premium_short: float = 0.0
    stop_loss_value: float = 0.0
    entry_costs: float = 0.0
    net_delta: float = 0.0
    net_gamma: float = 0.0
    net_theta: float = 0.0
    net_vega: float = 0.0
    db_trade_id: int = None

    # --- NEW: Entry Context Metadata ---
    ivr_at_entry: float = 0.0
    vix_at_entry: float = 0.0
    nifty_spot_at_entry: float = 0.0
    max_bid_ask_spread_pct: float = 0.0
    exit_costs: float = 0.0

    # --- NEW: Added for tracking after closure ---
    realized_pnl: float = 0.0
    exit_reason: str = ""
    exit_timestamp: Optional[datetime] = None
    snapshots: list[tuple] = field(default_factory=list)

    @property
    def net_entry_premium(self) -> float:
        """
        Net premium at entry.
        Positive = credit received (credit spread)
        Negative = debit paid (debit spread)
        """
        return self.entry_premium_short - self.entry_premium_long

    @property
    def current_spread_value(self) -> float:
        """
        Current mark-to-market value of the spread.
        For credit spreads: profit when this decreases
        For debit spreads:  profit when this increases
        """
        return self.current_premium_long - self.current_premium_short

    @property
    def net_entry_premium(self) -> float:
        """Net premium at entry (Credit is positive, Debit is negative)."""
        return self.entry_premium_short - self.entry_premium_long

    @property
    def net_exit_premium(self) -> float:
        """Net premium at exit (Credit is positive, Debit is negative). Needs to be set at closure."""
        # Note: We usually set current_premium_* at entry to last known prices
        # For historical report accuracy, we use the property.
        return self.current_premium_short - self.current_premium_long

    @property
    def net_current_premium(self) -> float:
        return self.current_premium_short - self.current_premium_long

    @property
    def unrealized_pnl(self) -> float:
        """
        Current unrealized P&L in ₹.
        Accounts for lot size and number of lots.
        Does not include transaction costs.
        """
        entry_value = self.entry_premium_long - self.entry_premium_short
        current_value = self.current_premium_long - self.current_premium_short
        return (current_value - entry_value) * self.lot_size * self.lots

    @property
    def gross_pnl(self) -> float:
        """Alias for unrealized_pnl for reporting at position close."""
        return self.unrealized_pnl

    @property
    def is_profitable(self) -> bool:
        """True if position is currently in profit."""
        return self.unrealized_pnl > 0


class PortfolioState:
    """
    Single source of truth for all runtime risk decisions.

    Lives entirely in memory. Updated synchronously at the
    moment of every fill — before any database write is queued.
    """

    def __init__(self):
        # Open positions registry
        self._positions: dict[str, Position] = {}

        # --- NEW: Closed positions archive ---
        self._closed_positions: dict[str, Position] = {}

        # Session P&L tracking
        self._realized_pnl: float = 0.0
        self._session_date: object = None
        self._starting_capital: float = 0.0

        # Trade count for THIS session (excludes recovered)
        self._new_trades_count: int = 0

        # Trade counter for position ID generation (may include recovered)
        self._trade_counter: int = 0

        # Session ID from database
        self._session_id: Optional[int] = None

        # Circuit breaker state
        self._circuit_breaker_tripped: bool = False
        self._circuit_breaker_time: datetime = None

    @property
    def total_capital(self) -> float:
        """Current session capital (Starting + Realized P&L)."""
        return self._starting_capital + self._realized_pnl

    def initialize_session(self, session_date, starting_capital: float) -> None:
        """
        Called at 08:45 AM to reset the session state.
        Clears all positions and P&L from the previous session.
        Attempt to restore session-specific realized P&L if this is a mid-day restart.
        """
        self._positions.clear()
        self._closed_positions.clear()
        
        # New: Attempt to restore TODAY'S realized profit from disk
        restored_realized = 0.0
        try:
            state_path = "data/capital_state.json"
            if os.path.exists(state_path):
                with open(state_path, "r") as f:
                    state = json.load(f)
                    # Only restore if the last update was today
                    last_ts = datetime.fromisoformat(state.get("last_updated", "2000-01-01"))
                    if last_ts.date() == date_type.today():
                        restored_realized = state.get("session_realized_pnl", 0.0)
                        starting_capital = state.get("session_starting_capital", starting_capital)
        except:
            pass

        self._realized_pnl = restored_realized
        self._session_date = session_date
        self._starting_capital = starting_capital
        self._trade_counter = 0
        self._new_trades_count = 0
        self._circuit_breaker_tripped = False
        self._circuit_breaker_time = None

        # --- RESILIENCE FIX: Auto-Recover Ghost Trades ---
        self._load_snapshot()

        logger.info(
            f"[PORTFOLIO STATE] Session initialized. "
            f"Date: {session_date} | "
            f"Capital: ₹{starting_capital:,.0f} | "
            f"Recovered: {len(self._positions)} positions"
        )

    def _save_snapshot(self):
        """Persists open positions to disk for crash recovery."""
        try:
            os.makedirs("data", exist_ok=True)
            data = {}
            for pid, pos in self._positions.items():
                data[pid] = {
                    "position_id": pos.position_id,
                    "symbol": pos.symbol,
                    "spread_type": pos.spread_type,
                    "long_strike": pos.long_strike,
                    "short_strike": pos.short_strike,
                    "option_type": pos.option_type,
                    "lots": pos.lots,
                    "lot_size": pos.lot_size,
                    "entry_timestamp": pos.entry_timestamp.isoformat(),
                    "expiry_date": pos.expiry_date.isoformat(),
                    "entry_premium_long": pos.entry_premium_long,
                    "entry_premium_short": pos.entry_premium_short,
                    "current_premium_long": pos.current_premium_long,
                    "current_premium_short": pos.current_premium_short,
                    "stop_loss_value": pos.stop_loss_value,
                    "entry_costs": pos.entry_costs,
                    "ivr_at_entry": pos.ivr_at_entry,
                    "vix_at_entry": pos.vix_at_entry,
                    "nifty_spot_at_entry": pos.nifty_spot_at_entry,
                    "max_bid_ask_spread_pct": pos.max_bid_ask_spread_pct,
                    "snapshots": [
                        (s[0].isoformat() if isinstance(s[0], datetime) else s[0], s[1], s[2], s[3], s[4], s[5], s[6])
                        for s in pos.snapshots
                    ] if pos.snapshots else [],
                }
            with open("data/ghost_snapshot.json", "w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"[PORTFOLIO STATE] Failed to save snapshot: {e}")

    def _save_capital(self):
        """Persists the current total capital to capital_state.json without wiping flags."""
        import json
        import os
        state_path = "data/capital_state.json"
        try:
            current_total = self._starting_capital + self._realized_pnl
            
            # 1. Load existing state to preserve flags
            state = {}
            if os.path.exists(state_path):
                with open(state_path, "r") as f:
                    state = json.load(f)
            
            # 2. Update only balance, realized P&L, and timestamp
            state["last_updated"] = datetime.now().isoformat()
            state["final_balance"] = round(current_total, 2)
            state["session_realized_pnl"] = round(self._realized_pnl, 2)
            state["session_starting_capital"] = round(self._starting_capital, 2)
            
            # 3. Save back
            with open(state_path, "w") as f:
                json.dump(state, f, indent=4)
            
            # 4. Sync to runtime_config for dashboard accuracy
            if runtime_config.risk:
                runtime_config.risk.total_capital = round(current_total, 2)
                
            logger.info(f"[PORTFOLIO STATE] Capital persisted: ₹{current_total:,.2f}")
        except Exception as e:
            logger.error(f"[PORTFOLIO STATE] Failed to persist capital: {e}")

    def _load_snapshot(self):
        """Loads open positions from disk if they belong to today."""
        path = "data/ghost_snapshot.json"
        if not os.path.exists(path):
            return

        try:
            with open(path, "r") as f:
                data = json.load(f)

            for pid, pdata in data.items():
                entry_dt = datetime.fromisoformat(pdata["entry_timestamp"])
                pos = Position(
                    position_id=pdata["position_id"],
                    symbol=pdata["symbol"],
                    spread_type=pdata["spread_type"],
                    long_strike=pdata["long_strike"],
                    short_strike=pdata["short_strike"],
                    option_type=pdata["option_type"],
                    lots=pdata["lots"],
                    lot_size=pdata["lot_size"],
                    entry_timestamp=entry_dt,
                    expiry_date=date_type.fromisoformat(pdata["expiry_date"]),
                    entry_premium_long=pdata["entry_premium_long"],
                    entry_premium_short=pdata["entry_premium_short"],
                    current_premium_long=pdata["current_premium_long"],
                    current_premium_short=pdata["current_premium_short"],
                    stop_loss_value=pdata["stop_loss_value"],
                    entry_costs=pdata["entry_costs"],
                    ivr_at_entry=pdata.get("ivr_at_entry", 0.0),
                    vix_at_entry=pdata.get("vix_at_entry", 0.0),
                    nifty_spot_at_entry=pdata.get("nifty_spot_at_entry", 0.0),
                    max_bid_ask_spread_pct=pdata.get("max_bid_ask_spread_pct", 0.0),
                )
                snapshots_raw = pdata.get("snapshots", [])
                pos.snapshots = [
                    (datetime.fromisoformat(s[0]), s[1], s[2], s[3], s[4], s[5], s[6])
                    for s in snapshots_raw
                ]
                self._positions[pid] = pos
                # Update counter so next ID doesn't collide
                try:
                    seq = int(pid.split("_")[1])
                    self._trade_counter = max(self._trade_counter, seq)
                except:
                    pass
        except Exception as e:
            logger.error(f"[PORTFOLIO STATE] Failed to load snapshot: {e}")

    def generate_position_id(self) -> str:
        """Generates a unique position ID for this session."""
        self._trade_counter += 1
        return f"{self._session_date.strftime('%Y%m%d')}" f"_{self._trade_counter:04d}"

    def add_position(self, position: Position) -> None:
        """
        Registers a new open position.
        Called immediately when a basket order is confirmed.
        """
        self._positions[position.position_id] = position
        self._new_trades_count += 1

        # --- NEW: Snapshot to disk instantly ---
        self._save_snapshot()

        logger.info(
            f"[PORTFOLIO STATE] Position opened: "
            f"{position.position_id} | "
            f"{position.spread_type} | "
            f"Long: {position.long_strike} | "
            f"Short: {position.short_strike} | "
            f"Lots: {position.lots} | "
            f"Net premium: ₹{position.net_entry_premium:.2f}"
        )

    def close_position(
        self,
        position_id: str,
        exit_premium_long: float,
        exit_premium_short: float,
        transaction_costs: float,
        exit_reason: str,
    ) -> float:
        """
        Closes a position, archives it, and records realized P&L.
        Returns the net P&L for this trade in ₹.
        """
        # We POP the position out of active positions
        position = self._positions.pop(position_id, None)

        if not position:
            logger.error(
                f"[PORTFOLIO STATE] Position {position_id} not found for closing."
            )
            return 0.0

        # --- NEW: Snapshot to disk instantly (Removing the closed trade) ---
        self._save_snapshot()

        # Calculate gross P&L
        entry_value = position.entry_premium_long - position.entry_premium_short
        exit_value = exit_premium_long - exit_premium_short
        gross_pnl = (exit_value - entry_value) * position.lot_size * position.lots
        net_pnl = gross_pnl - transaction_costs

        self._realized_pnl += net_pnl

        position.realized_pnl = net_pnl
        position.exit_reason = exit_reason
        position.exit_timestamp = datetime.now()
        position.exit_costs = transaction_costs - getattr(position, "entry_costs", 0.0)
        position.current_premium_long = exit_premium_long
        position.current_premium_short = exit_premium_short
        self._closed_positions[position_id] = position
        # ----------------------------------------------------

        logger.info(
            f"[PORTFOLIO STATE] Position closed: "
            f"{position_id} | "
            f"Reason: {exit_reason} | "
            f"Gross P&L: ₹{gross_pnl:,.2f} | "
            f"Costs: ₹{transaction_costs:.2f} | "
            f"Net P&L: ₹{net_pnl:,.2f} | "
            f"Session P&L: ₹{self._realized_pnl:,.2f}"
        )
        # --- NEW: Persist the new capital balance instantly ---
        self._save_capital()

        return net_pnl

    # --- NEW: Getter for the background worker ---
    def get_closed_position(self, position_id: str) -> Optional[Position]:
        """Returns a position from the closed archive."""
        return self._closed_positions.get(position_id)

    # ---------------------------------------------

    def update_mark_to_market(
        self,
        position_id: str,
        current_premium_long: float,
        current_premium_short: float,
    ) -> None:
        """
        Updates the current market prices for an open position.
        Called by the stop-loss monitor every 5 seconds.
        """
        position = self._positions.get(position_id)
        if position:
            position.current_premium_long = current_premium_long
            position.current_premium_short = current_premium_short

    def update_greeks(
        self,
        position_id: str,
        net_delta: float,
        net_gamma: float,
        net_theta: float,
        net_vega: float,
    ) -> None:
        """
        Updates Greeks for an open position.
        Called by greeks_monitor every 5 minutes.
        """
        position = self._positions.get(position_id)
        if position:
            position.net_delta = net_delta
            position.net_gamma = net_gamma
            position.net_theta = net_theta
            position.net_vega = net_vega

    def trip_circuit_breaker(self) -> None:
        """
        Trips the circuit breaker.
        Called by circuit_breaker.py when daily loss limit hit.
        Once tripped, cannot be reset until next session.
        """
        self._circuit_breaker_tripped = True
        self._circuit_breaker_time = datetime.now()
        logger.error(
            f"[PORTFOLIO STATE] Circuit breaker TRIPPED at "
            f"{self._circuit_breaker_time.strftime('%H:%M:%S')}. "
            f"Daily P&L: ₹{self.daily_pnl:,.2f}"
        )

    # ------------------------------------------------------------------
    # Properties — read by risk engine, never mutated externally
    # ------------------------------------------------------------------

    @property
    def open_positions(self) -> dict:
        """All currently open positions."""
        return dict(self._positions)

    @property
    def position_count(self) -> int:
        """Number of currently open positions."""
        return len(self._positions)

    @property
    def realized_pnl(self) -> float:
        """Total realized P&L for the session in ₹."""
        return self._realized_pnl

    @property
    def unrealized_pnl(self) -> float:
        """Total unrealized P&L across all open positions in ₹."""
        return sum(p.unrealized_pnl for p in self._positions.values())

    @property
    def daily_pnl(self) -> float:
        """
        Total session P&L = realized + unrealized.
        This is what the circuit breaker monitors.
        """
        return self._realized_pnl + self.unrealized_pnl

    @property
    def circuit_breaker_tripped(self) -> bool:
        """True if the circuit breaker has been tripped today."""
        return self._circuit_breaker_tripped

    @property
    def is_ready(self) -> bool:
        """True if the session has been initialized."""
        return self._session_date is not None


# Single instance — imported by all risk modules
portfolio_state = PortfolioState()
