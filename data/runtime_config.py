import asyncio
from dataclasses import dataclass, field
from datetime import date
from loguru import logger
from config.constants import (
    PCR_BULLISH,
    PCR_BEARISH,
    ATR_MULTIPLIER,
    MAX_CONCURRENT_POSITIONS,
    RISK_PER_TRADE_PCT,
    CIRCUIT_BREAKER_PCT,
    OPTIONS_QUEUE_SIZE,
    FUTURES_QUEUE_SIZE,
    SIGNAL_QUEUE_SIZE,
)


@dataclass
class InstrumentConfig:
    symbol: str
    lot_size: int
    strike_interval: int
    expiry_type: str
    next_expiry: date
    expiry_weekday: str
    instrument_tokens: dict = field(default_factory=dict)
    futures_token: int = None


@dataclass
class RiskConfig:
    total_capital: float
    max_risk_per_trade: float
    daily_circuit_breaker: float
    max_concurrent: int


@dataclass
class RegimeConfig:
    """
    Simplified Regime state.
    IVR entry/exit thresholds removed as per USER request.
    Only PCR and ATR multipliers remain from the original system.
    """
    pcr_bullish: float = PCR_BULLISH
    pcr_bearish: float = PCR_BEARISH
    atr_multiplier: float = ATR_MULTIPLIER


class RuntimeConfig:
    """
    Populated fresh every morning at 08:45 AM from
    Scrip Master CSV and live account balance.
    Single instance shared across all modules.
    """

    def __init__(self):
        self.instruments: dict[str, InstrumentConfig] = {}
        self.risk: RiskConfig = None
        self.regime: RegimeConfig = RegimeConfig()
        self.is_ready: bool = False

        # Live data queues
        self.options_queue: asyncio.Queue = None
        self.futures_queue: asyncio.Queue = None
        self.signal_queue: asyncio.Queue = None

    def init_queues(self):
        """
        Creates asyncio queues inside the running event loop.
        """
        self.options_queue = asyncio.Queue(maxsize=OPTIONS_QUEUE_SIZE)
        self.futures_queue = asyncio.Queue(maxsize=FUTURES_QUEUE_SIZE)
        self.signal_queue = asyncio.Queue(maxsize=SIGNAL_QUEUE_SIZE)
        logger.info(
            "[RUNTIME CONFIG] Asyncio queues initialised. "
            f"Options: {OPTIONS_QUEUE_SIZE} | "
            f"Futures: {FUTURES_QUEUE_SIZE} | "
            f"Signal: {SIGNAL_QUEUE_SIZE}"
        )

    def set_instrument(self, config: InstrumentConfig):
        """
        Stores instrument config.
        """
        self.instruments[config.symbol] = config
        logger.info(
            f"[RUNTIME CONFIG] {config.symbol} loaded — "
            f"Lot: {config.lot_size} | "
            f"Expiry: {config.next_expiry} ({config.expiry_weekday})"
        )

    def set_risk(self, total_capital: float, max_concurrent: int = None):
        """
        Computes risk parameters from live account balance.
        """
        concurrent = max_concurrent or MAX_CONCURRENT_POSITIONS
        self.risk = RiskConfig(
            total_capital=total_capital,
            max_risk_per_trade=round(total_capital * 0.30, 2),
            daily_circuit_breaker=round(total_capital * CIRCUIT_BREAKER_PCT, 2),
            max_concurrent=concurrent,
        )
        logger.info(
            f"[RUNTIME CONFIG] Risk loaded — "
            f"Capital: ₹{total_capital:,.0f} | "
            f"Circuit breaker: ₹{self.risk.daily_circuit_breaker:,.0f}"
        )

    def set_regime_thresholds(
        self,
        pcr_bullish: float = PCR_BULLISH,
        pcr_bearish: float = PCR_BEARISH,
        atr_multiplier: float = ATR_MULTIPLIER,
        **kwargs # Ignore legacy IVR arguments
    ):
        """
        Updates essential regime thresholds.
        Legacy IVR parameters are ignored for backtest parity.
        """
        self.regime = RegimeConfig(
            pcr_bullish=pcr_bullish,
            pcr_bearish=pcr_bearish,
            atr_multiplier=atr_multiplier,
        )
        logger.info(
            f"[RUNTIME CONFIG] Regime parameters updated — "
            f"PCR Bull: >{pcr_bullish} | PCR Bear: <{pcr_bearish} | "
            f"ATR mult: {atr_multiplier}"
        )

    def validate(self) -> bool:
        """
        Final validation before usage.
        """
        if not self.instruments: return False
        if self.risk is None: return False
        
        nifty = self.instruments.get("NIFTY")
        if not nifty or not nifty.instrument_tokens or nifty.futures_token is None:
            return False

        self.is_ready = True
        return True


# Single instance — imported by all modules
runtime_config = RuntimeConfig()
