from dataclasses import dataclass, field
from datetime import date
from abc import ABC, abstractmethod


@dataclass
class ContractSpec:
    symbol: str
    lot_size: int
    strike_interval: int
    expiry_type: str
    expiry_date: date
    instrument_tokens: dict = field(default_factory=dict)


class IndexContract(ABC):
    @abstractmethod
    def get_atm_strike(self, spot: float) -> int: ...


class NiftyWeeklyContract(IndexContract):
    """
    Phase 1 — SEBI compliant weekly contract.
    NSE only. Post SEBI circular November 20 2024.
    Lot size and expiry fetched dynamically from
    Scrip Master every morning — never hardcoded.
    """

    SYMBOL = "NIFTY"
    EXPIRY_TYPE = "WEEKLY"

    def get_atm_strike(self, spot: float, strike_interval: int) -> int:
        return round(spot / strike_interval) * strike_interval
