from datetime import datetime, date as date_type
from loguru import logger
from analytics.cost_model import cost_model
from persistence.write_buffer import write_buffer
from monitoring.alerting import alert_trade_entry
from risk.portfolio_state import portfolio_state, Position
from data.runtime_config import runtime_config


class GhostLedger:
    """
    Simulates order execution in LIVE_TRADING = False mode.

    Fill rules:
        BUY  fills at Ask + slippage buffer
        SELL fills at Bid - slippage buffer
        LTP is architecturally forbidden

    Cost rules:
        Entry costs recorded at fill time in Position.entry_costs
        Exit costs calculated at close time
        Total cost = entry_costs + exit_costs
        Net P&L = gross_pnl - total_cost

    Database rule:
        One record per trade written at CLOSE only
        Contains complete entry and exit data
        No partial records at entry time
    """

    async def fill_basket(
        self,
        basket: dict,
        market_data: dict,
        position_id: str,
        stop_loss_pts: float,
        entry_analytics: dict = None,
    ) -> bool:
        """
        Simulates filling a basket order at live prices.
        Records entry data in the Position object.
        Does NOT write to database at entry — write happens at close.
        """
        if not basket or not market_data:
            logger.error(
                "[GHOST LEDGER] Cannot fill — " "basket or market data is None."
            )
            return False

        legs = basket["legs"]
        lots = basket["lots"]
        lot_size = basket["lot_size"]
        long_leg = next(l for l in legs if l["leg"] == "LONG")
        short_leg = next(l for l in legs if l["leg"] == "SHORT")

        long_prices = self._get_leg_prices(
            long_leg["strike"], long_leg["option_type"], market_data
        )
        short_prices = self._get_leg_prices(
            short_leg["strike"], short_leg["option_type"], market_data
        )

        if long_prices is None or short_prices is None:
            logger.error(
                f"[GHOST LEDGER] Missing market data for legs. "
                f"Long: {long_leg['strike']}{long_leg['option_type']} "
                f"→ {long_prices} | "
                f"Short: {short_leg['strike']}{short_leg['option_type']} "
                f"→ {short_prices}"
            )
            return False

        long_fill = self._buy_price(long_prices["bid"], long_prices["ask"])
        short_fill = self._sell_price(short_prices["bid"], short_prices["ask"])

        if long_fill <= 0 or short_fill <= 0:
            logger.error(
                f"[GHOST LEDGER] Invalid fill prices. "
                f"Long: {long_fill} | Short: {short_fill}"
            )
            return False

        # Entry transaction costs
        entry_costs = cost_model.total_spread_cost(
            long_premium=long_fill,
            short_premium=short_fill,
            lot_size=lot_size,
            lots=lots,
            is_entry=True,
        )

        stop_loss_value = -(stop_loss_pts * lot_size * lots)

        position = Position(
            position_id=position_id,
            symbol=basket["symbol"],
            spread_type=basket["spread_type"],
            long_strike=long_leg["strike"],
            short_strike=short_leg["strike"],
            option_type=long_leg["option_type"],
            lots=lots,
            lot_size=lot_size,
            entry_timestamp=datetime.now(),
            expiry_date=basket["expiry"],
            entry_premium_long=long_fill,
            entry_premium_short=short_fill,
            current_premium_long=long_fill,
            current_premium_short=short_fill,
            stop_loss_value=stop_loss_value,
            entry_costs=entry_costs,
            ivr_at_entry=entry_analytics.get("ivr_at_entry", 0.0) if entry_analytics else 0.0,
            vix_at_entry=entry_analytics.get("vix_at_entry", 0.0) if entry_analytics else 0.0,
            nifty_spot_at_entry=entry_analytics.get("nifty_spot", 0.0) if entry_analytics else 0.0,
            max_bid_ask_spread_pct=entry_analytics.get("max_bid_ask_spread_pct", 0.0) if entry_analytics else 0.0,
        )

        portfolio_state.add_position(position)
        
        # --- ENRICHED TELEGRAM ALERT ---
        strikes_desc = (
            f"Long {position.long_strike}{position.option_type} @ ₹{position.entry_premium_long:.2f} | "
            f"Short {position.short_strike}{position.option_type} @ ₹{position.entry_premium_short:.2f}"
        )
        alert_trade_entry(
            direction="ENTRY",
            strategy=position.spread_type,
            strikes=strikes_desc,
            lots=position.lots,
            net_premium=position.net_entry_premium,
            regime=f"Spot: {position.nifty_spot_at_entry:,.2f}"
        )

        logger.info(
            f"[GHOST LEDGER] FILLED: {position_id} | "
            f"{basket['spread_type']} | "
            f"Long {long_leg['strike']}{long_leg['option_type']} "
            f"@ ₹{long_fill:.2f} (Ask+slip) | "
            f"Short {short_leg['strike']}{short_leg['option_type']} "
            f"@ ₹{short_fill:.2f} (Bid-slip) | "
            f"Lots: {lots} | "
            f"Entry costs: ₹{entry_costs:.2f} | "
            f"Stop: ₹{stop_loss_value:,.2f}"
        )
        return True

    async def close_basket(
        self,
        position_id: str,
        market_data: dict,
        exit_reason: str,
    ) -> bool:
        """
        Simulates closing a position at live prices.
        Writes ONE complete trade record to the database.
        Includes both entry and exit data in a single record.
        """
        position = portfolio_state.open_positions.get(position_id)
        if not position:
            logger.error(
                f"[GHOST LEDGER] Position {position_id} " f"not found for closing."
            )
            return False

        long_prices = self._get_leg_prices(
            position.long_strike, position.option_type, market_data
        )
        short_prices = self._get_leg_prices(
            position.short_strike, position.option_type, market_data
        )

        if long_prices is None or short_prices is None:
            logger.warning(
                f"[GHOST LEDGER] Missing market data for exit. "
                f"Using last known prices."
            )
            exit_long = position.current_premium_long
            exit_short = position.current_premium_short
        else:
            # EXIT long leg = SELL = Bid - slippage
            exit_long = self._sell_price(long_prices["bid"], long_prices["ask"])
            # EXIT short leg = BUY BACK = Ask + slippage
            exit_short = self._buy_price(short_prices["bid"], short_prices["ask"])

        # Exit transaction costs
        exit_costs = cost_model.total_spread_cost(
            long_premium=exit_long,
            short_premium=exit_short,
            lot_size=position.lot_size,
            lots=position.lots,
            is_entry=False,
        )

        # Total costs = entry costs + exit costs
        total_costs = position.entry_costs + exit_costs

        # Close in portfolio state
        net_pnl = portfolio_state.close_position(
            position_id=position_id,
            exit_premium_long=exit_long,
            exit_premium_short=exit_short,
            transaction_costs=total_costs,
            exit_reason=exit_reason,
        )

        gross_pnl = net_pnl + total_costs

        # Write ONE complete trade record to database
        await write_buffer.put(
            "trades",
            (
                portfolio_state._session_id,  # session_id
                position.position_id,         # position_id
                position.entry_timestamp,     # entry_timestamp
                datetime.now(),               # exit_timestamp
                position.expiry_date,         # expiry_date — date object
                position.symbol,              # symbol
                position.long_strike,
                position.short_strike,
                position.option_type,
                position.lots,
                position.entry_premium_long,
                position.entry_premium_short,
                position.current_premium_long,  # exit_premium_long
                position.current_premium_short, # exit_premium_short
                position.unrealized_pnl,        # gross_pnl (at exit = real pnl)
                total_costs,
                position.unrealized_pnl - total_costs,  # net_pnl
                exit_reason,
            ),
        )

        # --- NEW: Write buffered snapshots ---
        for snap in position.snapshots:
            # snap order: timestamp, pnl, delta, theta, gamma, vega, iv
            await write_buffer.put(
                "positions",
                (
                    position.position_id,  # Used as lookup key in subquery
                    snap[0],  # timestamp
                    snap[1],  # pnl (mark_to_market)
                    snap[2],  # delta
                    snap[3],  # theta
                    snap[4],  # gamma
                    snap[5],  # vega
                    snap[6],  # iv
                )
            )

        logger.info(
            f"[GHOST LEDGER] Database entries queued for {position.position_id} "
            f"({len(position.snapshots)} snapshots attached)"
        )
        logger.info(
            f"[GHOST LEDGER] CLOSED: {position_id} | "
            f"Reason: {exit_reason} | "
            f"Exit long @ ₹{exit_long:.2f} | "
            f"Exit short @ ₹{exit_short:.2f} | "
            f"Entry costs: ₹{position.entry_costs:.2f} | "
            f"Exit costs: ₹{exit_costs:.2f} | "
            f"Total costs: ₹{total_costs:.2f} | "
            f"Net P&L: ₹{net_pnl:,.2f}"
        )
        return True

    def _get_leg_prices(self, strike: int, option_type: str, market_data: dict) -> dict:
        """
        Extracts Bid and Ask for a leg from market data.
        Returns None if Bid or Ask is missing or zero.
        LTP keys are ignored even if present.
        """
        strike_data = market_data.get(strike)
        if not strike_data:
            return None

        leg_data = strike_data.get(option_type)
        if not leg_data:
            return None

        bid = leg_data.get("bid", 0.0)
        ask = leg_data.get("ask", 0.0)

        if bid <= 0 or ask <= 0:
            return None

        return {"bid": bid, "ask": ask}

    def _buy_price(self, bid: float, ask: float) -> float:
        """BUY fills at Ask + slippage. LTP never used."""
        slippage = cost_model.calculate_live_slippage(bid, ask)
        return round(ask + slippage, 2)

    def _sell_price(self, bid: float, ask: float) -> float:
        """SELL fills at Bid - slippage. LTP never used."""
        slippage = cost_model.calculate_live_slippage(bid, ask)
        return round(bid - slippage, 2)


# Single instance
ghost_ledger = GhostLedger()
