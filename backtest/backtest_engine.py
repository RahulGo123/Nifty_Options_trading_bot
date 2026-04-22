import math
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from backtest.slippage_model import compute_entry_fill, compute_exit_fill, SpreadFill

# ---------------------------------------------------------------------------
# Core Constants
# ---------------------------------------------------------------------------
LOT_SIZE = 65
STRIKE_INTERVAL = 50
MAX_POSITIONS = 2
ATR_PERIOD = 14
ATR_CANDLE_MIN = 15

MARKET_OPEN = time(9, 15)
KILL_TIME = time(15, 10)
NEAR_EXPIRY_T = time(13, 30)
ENTRY_CUTOFF = time(14, 30)


@dataclass
class BacktestParams:
    min_vix: float = 10.5
    max_vix: float = 19.0
    vwap_pullback_tolerance: float = 0.0045
    atr_multiplier: float = 2.0
    profit_target: float = 0.85  # Theta Harvest Target
    theta_days: int = 2
    theta_min_gain: float = 0.15
    risk_per_trade: float = 30_000.0

    spread_width: int = 6
    min_days_to_expiry: int = 0
    allowed_dow: Tuple[int, ...] = (0, 1, 2, 3, 4)

    margin_per_lot: float = 65_000.0
    max_lots_override: int = 0

    def label(self) -> str:
        return (
            f"vix{self.min_vix:.1f}_vwap{self.vwap_pullback_tolerance:.4f}"
            f"_atr{self.atr_multiplier:.1f}_pt{self.profit_target:.2f}"
        )


@dataclass
class BacktestTrade:
    trade_id: int
    episode_expiry: date
    trading_date: date
    direction: str
    spread_type: str
    long_strike: int
    short_strike: int
    option_type: str
    lots: int
    entry_time: datetime
    entry_long: float
    entry_short: float
    entry_costs: float
    stop_value: float
    max_loss_value: float
    exit_time: Optional[datetime] = None
    exit_long: Optional[float] = None
    exit_short: Optional[float] = None
    entry_vix: float = 0.0
    exit_costs: float = 0.0
    exit_reason: str = ""

    @property
    def net_pnl(self) -> Optional[float]:
        if self.exit_time is None:
            return None
        entry_credit = (self.entry_short - self.entry_long) * LOT_SIZE * self.lots
        exit_debit = (self.exit_short - self.exit_long) * LOT_SIZE * self.lots
        gross = entry_credit - exit_debit
        return gross - self.entry_costs - self.exit_costs

    @property
    def net_entry_premium(self) -> float:
        return self.entry_short - self.entry_long

    def unrealized_pnl(self, lc: float, sc: float) -> float:
        entry_credit = self.entry_short - self.entry_long
        current_debit = sc - lc
        return (entry_credit - current_debit) * LOT_SIZE * self.lots

    def close(self, t: datetime, fill: SpreadFill, reason: str) -> None:
        self.exit_time = t
        self.exit_long = fill.long_fill
        self.exit_short = fill.short_fill
        self.exit_costs = fill.costs
        self.exit_reason = reason


@dataclass
class EpisodeState:
    expiry: date
    open_trades: List[BacktestTrade] = field(default_factory=list)
    trade_id_counter: int = 0
    vwap_num: float = 0.0
    vwap_denom: float = 0.0
    atr_value: float = 0.0
    atr_candle_opens: List[float] = field(default_factory=list)
    atr_candle_highs: List[float] = field(default_factory=list)
    atr_candle_lows: List[float] = field(default_factory=list)
    atr_candle_closes: List[float] = field(default_factory=list)
    atr_bar_ticks: int = 0

    morning_fired: bool = False
    afternoon_fired: bool = False

    def next_trade_id(self) -> int:
        self.trade_id_counter += 1
        return self.trade_id_counter

    @property
    def position_count(self) -> int:
        return len(self.open_trades)


def atm_strike(spot: float) -> int:
    return round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL


def update_atr(state: EpisodeState, fut_close: float) -> float:
    state.atr_bar_ticks += 1
    if state.atr_bar_ticks == 1:
        state.atr_candle_opens.append(fut_close)
        state.atr_candle_highs.append(fut_close)
        state.atr_candle_lows.append(fut_close)
    else:
        state.atr_candle_highs[-1] = max(state.atr_candle_highs[-1], fut_close)
        state.atr_candle_lows[-1] = min(state.atr_candle_lows[-1], fut_close)

    if state.atr_bar_ticks >= ATR_CANDLE_MIN:
        state.atr_candle_closes.append(fut_close)
        state.atr_bar_ticks = 0
        n = len(state.atr_candle_closes)
        if n >= 2:
            h = np.array(state.atr_candle_highs[-min(n, ATR_PERIOD + 1) :])
            l = np.array(state.atr_candle_lows[-min(n, ATR_PERIOD + 1) :])
            c = np.array(state.atr_candle_closes[-min(n, ATR_PERIOD + 1) :])
            h1, l1, pc = h[1:], l[1:], c[:-1]
            m = min(len(h1), len(l1), len(pc))
            if m > 0:
                tr = np.maximum(
                    h1[-m:] - l1[-m:],
                    np.maximum(np.abs(h1[-m:] - pc[-m:]), np.abs(l1[-m:] - pc[-m:])),
                )
                if len(tr) >= ATR_PERIOD:
                    prev = (
                        state.atr_value
                        if state.atr_value > 0
                        else float(np.mean(tr[:ATR_PERIOD]))
                    )
                    state.atr_value = (prev * (ATR_PERIOD - 1) + tr[-1]) / ATR_PERIOD
                elif len(tr) > 0:
                    state.atr_value = float(np.mean(tr))

        state.atr_candle_opens.append(fut_close)
        state.atr_candle_highs.append(fut_close)
        state.atr_candle_lows.append(fut_close)
    return state.atr_value


def compute_max_loss(
    long_fill: float, short_fill: float, spread_width_pts: int, lots: int
) -> float:
    net_credit = short_fill - long_fill
    if net_credit > 0:
        max_loss_per_unit = spread_width_pts - net_credit
    else:
        max_loss_per_unit = abs(net_credit)
    return -(max_loss_per_unit * LOT_SIZE * lots)


def _open_position(
    state: EpisodeState,
    all_trades: List[BacktestTrade],
    ts_dt: datetime,
    atm: int,
    stype: str,
    option_closes: dict,
    params: BacktestParams,
    expiry: date,
    current_vix: float,
) -> None:
    width_pts = params.spread_width * STRIKE_INTERVAL
    buffer_pts = STRIKE_INTERVAL

    if stype == "CREDIT_PUT":
        otype, short_s, long_s = "PE", atm - buffer_pts, atm - buffer_pts - width_pts
        direction = "DELTA_NEUTRAL"
    else:
        otype, short_s, long_s = "CE", atm + buffer_pts, atm + buffer_pts + width_pts
        direction = "DELTA_NEUTRAL"

    lc = option_closes.get((long_s, otype))
    sc = option_closes.get((short_s, otype))
    if not lc or not sc or lc <= 0 or sc <= 0:
        return

    # --- THE MATHEMATICALLY CORRECT SIZING LOGIC ---
    # 1. Structural risk ceiling (Per Condor)
    max_risk_per_condor_rupees = width_pts * LOT_SIZE
    condors_by_risk = int(params.risk_per_trade / max_risk_per_condor_rupees)

    # 2. Margin Ceiling
    if hasattr(params, "max_lots_override") and params.max_lots_override > 0:
        condors_by_margin = params.max_lots_override
    else:
        # Fallback if params aren't passed down dynamically
        MAX_USABLE_MARGIN = 90_000.0
        condors_by_margin = int(MAX_USABLE_MARGIN / params.margin_per_lot)

    # 3. Final Execution Size (THE TRUTH)
    lots = min(condors_by_risk, condors_by_margin)

    if condors_by_margin < 1 or lots <= 0:
        return
    # -----------------------------------------------

    fill = compute_entry_fill(lc, sc, LOT_SIZE, lots)
    atr_stop = -(state.atr_value * params.atr_multiplier * LOT_SIZE * lots)
    max_loss = compute_max_loss(fill.long_fill, fill.short_fill, width_pts, lots)
    stop_value = max(atr_stop, max_loss)

    t = BacktestTrade(
        trade_id=state.next_trade_id(),
        episode_expiry=expiry,
        trading_date=ts_dt.date(),
        direction=direction,
        spread_type=stype,
        long_strike=long_s,
        short_strike=short_s,
        option_type=otype,
        lots=lots,
        entry_time=ts_dt,
        entry_long=fill.long_fill,
        entry_short=fill.short_fill,
        entry_costs=fill.costs,
        entry_vix=round(current_vix * 100, 2),
        stop_value=stop_value,
        max_loss_value=max_loss,
    )
    state.open_trades.append(t)
    all_trades.append(t)


def _check_exits(
    state: EpisodeState,
    ts_dt: datetime,
    option_closes: dict,
    params: BacktestParams,
    force_reason: Optional[str] = None,
) -> None:
    for trade in list(state.open_trades):
        lc = (
            option_closes.get((trade.long_strike, trade.option_type))
            or trade.entry_long
        )
        sc = (
            option_closes.get((trade.short_strike, trade.option_type))
            or trade.entry_short
        )
        lc, sc = (lc if lc > 0 else trade.entry_long), (
            sc if sc > 0 else trade.entry_short
        )

        if force_reason:
            fill = compute_exit_fill(lc, sc, LOT_SIZE, trade.lots)
            trade.close(ts_dt, fill, force_reason)
            state.open_trades.remove(trade)
            continue

        upnl = trade.unrealized_pnl(lc, sc)

        if upnl <= trade.stop_value:
            fill = compute_exit_fill(lc, sc, LOT_SIZE, trade.lots)
            trade.close(ts_dt, fill, "STOP_LOSS")
            state.open_trades.remove(trade)
            continue

        basis = abs(trade.net_entry_premium) * LOT_SIZE * trade.lots
        if basis <= 0:
            continue

        # Calculate captured profit percentage
        ratio = upnl / basis

        # FEATURE #10: Theta Harvester
        if ratio >= params.profit_target:
            trade.close(
                ts_dt, compute_exit_fill(lc, sc, LOT_SIZE, trade.lots), "THETA_HARVEST"
            )
            state.open_trades.remove(trade)
            continue

        days = (ts_dt.date() - trade.entry_time.date()).days
        if days >= params.theta_days and abs(ratio) < params.theta_min_gain:
            trade.close(
                ts_dt,
                compute_exit_fill(lc, sc, LOT_SIZE, trade.lots),
                "THETA_STAGNATION",
            )
            state.open_trades.remove(trade)


def precompute_episode(
    df, sma_series, excluded_days
) -> Tuple[Optional[date], List[Dict]]:
    if df.empty:
        return None, []

    if "instrument_type" not in df.columns:
        if "inst_type" in df.columns:
            df.rename(columns={"inst_type": "instrument_type"}, inplace=True)
        elif "option_type" in df.columns:
            df["instrument_type"] = df["option_type"].apply(
                lambda x: (
                    "FUT"
                    if pd.isna(x) or str(x).upper() in ["XX", "FUT", "NONE", ""]
                    else "OPT"
                )
            )

    if "ts" not in df.columns:
        df = df.reset_index()
        for col in ["ts", "index", "timestamp", "datetime", "Date"]:
            if col in df.columns and "ts" not in df.columns:
                df.rename(columns={col: "ts"}, inplace=True)

    raw_expiry = df["expiry"].iloc[0]
    if hasattr(raw_expiry, "date"):
        expiry = raw_expiry.date()
    elif isinstance(raw_expiry, date):
        expiry = raw_expiry
    else:
        expiry = pd.to_datetime(raw_expiry).date()

    if expiry in excluded_days:
        return None, []

    bars = []
    for ts, grp in df.groupby("ts"):
        if isinstance(ts, str):
            ts = pd.to_datetime(ts)
        ts_dt = ts.to_pydatetime()
        td_date = ts_dt.date()
        ts_time = ts_dt.time()

        if td_date in excluded_days or ts_time < MARKET_OPEN:
            continue

        try:
            sma_val = float(sma_series.loc[td_date])
        except KeyError:
            sma_val = 0.0

        fut_row = grp[grp["instrument_type"] == "FUT"]
        if fut_row.empty:
            continue
        fut_c = fut_row["close"].iloc[0]
        fut_v = fut_row["volume"].iloc[0]

        spot = fut_c
        atm = atm_strike(spot)
        days_to_expiry = (expiry - td_date).days
        is_exp = days_to_expiry == 0

        opt_rows = grp[grp["instrument_type"] == "OPT"]
        opt_c = {}
        iv_approx = 0.0
        atm_ce = atm_pe = None

        for _, r in opt_rows.iterrows():
            st, ot, c = r["strike"], r["option_type"], r["close"]
            opt_c[(st, ot)] = c
            if st == atm:
                if ot == "CE":
                    atm_ce = c
                elif ot == "PE":
                    atm_pe = c

        if atm_ce and atm_pe and days_to_expiry > 0:
            iv_approx = ((atm_ce + atm_pe) / spot) * math.sqrt(365 / days_to_expiry)

        bars.append(
            {
                "ts": ts_dt,
                "ts_time": ts_time,
                "trading_date": td_date,
                "days_to_expiry": days_to_expiry,
                "is_expiry_day": is_exp,
                "macro_sma": sma_val,
                "fut_close": fut_c,
                "fut_volume": fut_v,
                "spot": spot,
                "atm": atm,
                "iv_approx": iv_approx,
                "option_closes": opt_c,
            }
        )
    return expiry, bars


def run_episode_precomputed(
    expiry: date, bars: List[Dict], params: BacktestParams
) -> List[BacktestTrade]:
    if not bars:
        return []
    state = EpisodeState(expiry=expiry)
    all_trades: List[BacktestTrade] = []
    current_td: Optional[date] = None

    for bar in bars:
        ts_dt, ts_time, td_date = bar["ts"], bar["ts_time"], bar["trading_date"]
        is_exp, fut_c, fut_v = bar["is_expiry_day"], bar["fut_close"], bar["fut_volume"]
        atm, iv = bar["atm"], bar["iv_approx"]
        opt_c = bar["option_closes"]

        if td_date != current_td:
            current_td = td_date
            state.vwap_num = state.vwap_denom = 0
            state.morning_fired = state.afternoon_fired = False

        if is_exp and ts_time >= NEAR_EXPIRY_T:
            _check_exits(state, ts_dt, opt_c, params, "NEAR_EXPIRY_FORCED_EXIT")
            break
        if is_exp and ts_time >= KILL_TIME:
            _check_exits(state, ts_dt, opt_c, params, "HARD_KILL")
            break

        effective_v = fut_v if fut_v > 0 else 1.0
        state.vwap_num += fut_c * effective_v
        state.vwap_denom += effective_v
        current_vwap = state.vwap_num / state.vwap_denom

        update_atr(state, fut_c)
        _check_exits(state, ts_dt, opt_c, params)

        if (iv * 100) < params.min_vix or (iv * 100) > params.max_vix:
            continue

        if (
            state.position_count >= MAX_POSITIONS
            or state.atr_value <= 0
            or bar["days_to_expiry"] < params.min_days_to_expiry
            or td_date.weekday() not in params.allowed_dow
        ):
            continue

        vwap_distance = abs(fut_c - current_vwap) / current_vwap
        if vwap_distance > params.vwap_pullback_tolerance:
            continue

        if ts_time >= ENTRY_CUTOFF:
            continue

        is_afternoon = ts_time >= time(13, 0)
        can_fire_morning = not state.morning_fired and not is_afternoon
        can_fire_afternoon = (
            not state.afternoon_fired
            and is_afternoon
            and state.position_count == 0
            and not is_exp
        )

        if can_fire_morning or can_fire_afternoon:
            _open_position(
                state,
                all_trades,
                ts_dt,
                atm,
                "CREDIT_PUT",
                opt_c,
                params,
                expiry,
                (iv * 100),
            )
            _open_position(
                state,
                all_trades,
                ts_dt,
                atm,
                "CREDIT_CALL",
                opt_c,
                params,
                expiry,
                (iv * 100),
            )

            if is_afternoon:
                state.afternoon_fired = True
            else:
                state.morning_fired = True

    if state.open_trades and bars:
        _check_exits(
            state, bars[-1]["ts"], bars[-1]["option_closes"], params, "EPISODE_END"
        )

    return all_trades


def run_episode(
    fp: Path, params: BacktestParams, sma_series, excluded_days
) -> List[BacktestTrade]:
    df = pd.read_parquet(fp, engine="pyarrow")
    exp, bars = precompute_episode(df, sma_series, excluded_days)
    if exp and bars:
        return run_episode_precomputed(exp, bars, params)
    return []
