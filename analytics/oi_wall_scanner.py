from loguru import logger
from data.runtime_config import runtime_config


class OIWallScanner:
    """
    Scans the options chain to find the Max OI strikes.

    The strike with the highest Total Call OI above ATM
    is the resistance wall — used as the short leg of
    Bull Call Spreads.

    The strike with the highest Total Put OI below ATM
    is the support wall — used as the short leg of
    Bear Put Spreads.

    Updated every 5 minutes. The OI wall can shift
    intraday as writers adjust positions — this is why
    it is not calculated once at open and forgotten.

    The shifting wall is one of the three reasons
    Bhavcopy (EOD data) cannot backtest this system —
    you cannot know where the wall was at 11:00 AM
    from a 3:30 PM snapshot.
    """

    def __init__(self):
        self._call_wall_strike: int = None
        self._put_wall_strike: int = None
        self._call_wall_oi: int = 0
        self._put_wall_oi: int = 0
        self._last_atm: int = None

    def update(
        self,
        atm_strike: int,
        oi_data: dict,
    ) -> None:
        """
        Scans OI data for the Max OI walls.

        oi_data structure:
        {
            strike: {
                'CE': {'total_oi': int},
                'PE': {'total_oi': int},
            }
        }

        Only scans strikes above ATM for Call wall
        and below ATM for Put wall. At-the-money and
        opposite-side strikes are ignored.
        """
        self._last_atm = atm_strike

        max_call_oi = 0
        max_put_oi = 0
        call_wall = None
        put_wall = None

        for strike, pair in oi_data.items():
            if strike > atm_strike:
                # Look for Call resistance above ATM
                ce_oi = pair.get("CE", {}).get("total_oi", 0)
                if ce_oi > max_call_oi:
                    max_call_oi = ce_oi
                    call_wall = strike

            elif strike < atm_strike:
                # Look for Put support below ATM
                pe_oi = pair.get("PE", {}).get("total_oi", 0)
                if pe_oi > max_put_oi:
                    max_put_oi = pe_oi
                    put_wall = strike

        prev_call = self._call_wall_strike
        prev_put = self._put_wall_strike

        self._call_wall_strike = call_wall
        self._call_wall_oi = max_call_oi
        self._put_wall_strike = put_wall
        self._put_wall_oi = max_put_oi

        # Log when the walls shift — important for position management
        if call_wall != prev_call:
            logger.info(
                f"[OI WALL] Call wall shifted: "
                f"{prev_call} → {call_wall} "
                f"(OI: {max_call_oi:,})"
            )
        if put_wall != prev_put:
            logger.info(
                f"[OI WALL] Put wall shifted: "
                f"{prev_put} → {put_wall} "
                f"(OI: {max_put_oi:,})"
            )

    @property
    def call_wall(self) -> int:
        """
        Strike with highest Call OI above ATM.
        Short leg for Bull Call Spreads.
        """
        return self._call_wall_strike

    @property
    def put_wall(self) -> int:
        """
        Strike with highest Put OI below ATM.
        Short leg for Bear Put Spreads.
        """
        return self._put_wall_strike

    @property
    def is_ready(self) -> bool:
        """
        True if both walls have been identified.
        The system will not construct spreads until
        both walls are known.
        """
        return self._call_wall_strike is not None and self._put_wall_strike is not None


# Single instance
oi_wall_scanner = OIWallScanner()
