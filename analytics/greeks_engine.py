import numpy as np
from scipy.stats import norm
from loguru import logger
from config.constants import DEFAULT_RISK_FREE_RATE


class GreeksEngine:
    """
    Vectorized Black-Scholes-Merton Greeks calculator.

    Computes Delta, Gamma, Theta, Vega, and Implied Volatility
    for all ATM ± 5 strikes simultaneously using NumPy arrays.

    Why vectorized?
    Running BSM iteratively (one strike at a time) on every
    tick would block Python for several milliseconds per batch.
    NumPy operates at C speed on entire arrays — computing
    11 strikes takes roughly the same time as computing 1.

    This engine runs in a separate worker process so it
    never touches the asyncio event loop.

    Risk-free rate:
    Uses the 91-day Indian T-bill yield as a proxy.
    Passed in via each batch from the main process so the
    worker always uses the most recently updated rate.
    Fallback: DEFAULT_RISK_FREE_RATE from constants.py.
    Updated weekly after RBI T-bill auctions.
    """

    MAX_IV_ITERATIONS = 100  # Newton-Raphson max iterations
    IV_TOLERANCE = 1e-6  # Convergence threshold
    MIN_IV = 0.001  # 0.1% floor
    MAX_IV = 10.0  # 1000% ceiling

    def __init__(self, risk_free_rate: float = None):
        self.risk_free_rate = (
            risk_free_rate if risk_free_rate is not None else DEFAULT_RISK_FREE_RATE
        )

    def compute_batch(
        self,
        spot: float,
        strikes: np.ndarray,
        premiums: np.ndarray,
        option_types: np.ndarray,
        time_to_expiry: float,
        risk_free_rate: float = None,
    ) -> dict:
        """
        Main entry point for the worker process.

        Computes IV and all Greeks for a batch of strikes
        simultaneously.

        Parameters:
            spot:           Current Nifty spot price
            strikes:        Array of strike prices [N]
            premiums:       Array of option premiums (mid-price) [N]
            option_types:   Array of 'CE' or 'PE' strings [N]
            time_to_expiry: Days to expiry / 365 (in years)
            risk_free_rate: Optional override — passed from main
                            process so worker always has the
                            latest RBI T-bill yield.

        Returns a dict with arrays of results, one value
        per strike in the input arrays.
        """
        # Update risk-free rate if passed in from main process
        if risk_free_rate is not None:
            self.risk_free_rate = risk_free_rate

        if time_to_expiry <= 0:
            logger.warning(
                "[GREEKS ENGINE] Time to expiry is zero or negative. "
                "Greeks computation skipped."
            )
            return self._empty_result(len(strikes))

        # Convert option types to BSM flags
        # Call = 1, Put = -1
        flags = np.where(np.array(option_types) == "CE", 1.0, -1.0)

        # Compute implied volatility for all strikes at once
        ivs = self._compute_iv_vectorized(
            spot, strikes, premiums, flags, time_to_expiry
        )

        # Compute all Greeks using the resolved IVs
        deltas, gammas, thetas, vegas = self._compute_greeks_vectorized(
            spot, strikes, ivs, flags, time_to_expiry
        )

        return {
            "strikes": strikes,
            "iv": ivs,
            "delta": deltas,
            "gamma": gammas,
            "theta": thetas,
            "vega": vegas,
        }

    def _d1(
        self, spot: np.ndarray, strike: np.ndarray, iv: np.ndarray, t: float
    ) -> np.ndarray:
        """
        Computes the d1 term used in BSM.

        d1 = [ln(S/K) + (r + σ²/2) × T] / (σ × √T)

        Where:
            S = spot price
            K = strike price
            r = risk-free rate
            σ = implied volatility
            T = time to expiry in years
        """
        return (np.log(spot / strike) + (self.risk_free_rate + 0.5 * iv**2) * t) / (
            iv * np.sqrt(t)
        )

    def _d2(self, d1: np.ndarray, iv: np.ndarray, t: float) -> np.ndarray:
        """
        d2 = d1 - σ × √T
        """
        return d1 - iv * np.sqrt(t)

    def _bsm_price(
        self,
        spot: np.ndarray,
        strike: np.ndarray,
        iv: np.ndarray,
        flags: np.ndarray,
        t: float,
    ) -> np.ndarray:
        """
        Computes the theoretical BSM option price for all
        strikes simultaneously.

        For a Call:  S × N(d1) - K × e^(-rT) × N(d2)
        For a Put:   K × e^(-rT) × N(-d2) - S × N(-d1)

        The flag (+1 for Call, -1 for Put) and the unified
        formula below handle both types without branching.
        """
        d1 = self._d1(spot, strike, iv, t)
        d2 = self._d2(d1, iv, t)
        discount = np.exp(-self.risk_free_rate * t)

        price = flags * (
            spot * norm.cdf(flags * d1) - strike * discount * norm.cdf(flags * d2)
        )
        return price

    def _compute_iv_vectorized(
        self,
        spot: float,
        strikes: np.ndarray,
        premiums: np.ndarray,
        flags: np.ndarray,
        t: float,
    ) -> np.ndarray:
        """
        Newton-Raphson IV solver, vectorized across all strikes.

        Starting with an initial IV guess, it iteratively refines
        the estimate using:

            IV_new = IV_old - (BSM_price(IV) - Market_price) / Vega

        This converges quadratically — each iteration roughly
        doubles the number of correct decimal places.

        Strikes that fail to converge (very deep ITM or OTM
        options with near-zero premiums) are clamped to the
        MIN_IV or MAX_IV bounds rather than returning NaN.
        """
        S = np.full_like(strikes, spot, dtype=float)

        # Initial IV guess — Brenner & Subrahmanyam approximation
        iv = np.sqrt(2 * np.pi / t) * premiums / S
        iv = np.clip(iv, self.MIN_IV, self.MAX_IV)

        for _ in range(self.MAX_IV_ITERATIONS):
            prices = self._bsm_price(S, strikes, iv, flags, t)
            diff = prices - premiums

            # Vega = S × N'(d1) × √T (same for calls and puts)
            d1 = self._d1(S, strikes, iv, t)
            vega = S * norm.pdf(d1) * np.sqrt(t)

            # Avoid division by near-zero vega
            safe_vega = np.where(np.abs(vega) < 1e-10, 1e-10, vega)

            # Newton-Raphson update step
            iv_new = iv - diff / safe_vega
            iv_new = np.clip(iv_new, self.MIN_IV, self.MAX_IV)

            # Check convergence
            if np.all(np.abs(iv_new - iv) < self.IV_TOLERANCE):
                break

            iv = iv_new

        return iv

    def _compute_greeks_vectorized(
        self,
        spot: float,
        strikes: np.ndarray,
        ivs: np.ndarray,
        flags: np.ndarray,
        t: float,
    ) -> tuple:
        """
        Computes Delta, Gamma, Theta, Vega for all strikes
        simultaneously using the resolved IV values.

        Delta:  Rate of change of option price vs spot price
                Call Delta ∈ (0, 1), Put Delta ∈ (-1, 0)

        Gamma:  Rate of change of Delta vs spot price
                Same formula for calls and puts

        Theta:  Daily time decay in ₹ per day
                Almost always negative

        Vega:   Sensitivity to 1% change in implied volatility
        """
        S = np.full_like(strikes, spot, dtype=float)
        d1 = self._d1(S, strikes, ivs, t)
        d2 = self._d2(d1, ivs, t)
        discount = np.exp(-self.risk_free_rate * t)
        sqrt_t = np.sqrt(t)

        # Delta
        delta = flags * norm.cdf(flags * d1)

        # Gamma
        gamma = norm.pdf(d1) / (S * ivs * sqrt_t)

        # Theta (per day)
        theta = (
            -(S * norm.pdf(d1) * ivs) / (2 * sqrt_t)
            - flags * self.risk_free_rate * strikes * discount * norm.cdf(flags * d2)
        ) / 365

        # Vega (for 1% move in IV)
        vega = S * norm.pdf(d1) * sqrt_t / 100

        return delta, gamma, theta, vega

    def _empty_result(self, n: int) -> dict:
        """Returns zero arrays when computation cannot proceed."""
        zeros = np.zeros(n)
        return {
            "strikes": zeros,
            "iv": zeros,
            "delta": zeros,
            "gamma": zeros,
            "theta": zeros,
            "vega": zeros,
        }


# Single instance — used by the worker process
greeks_engine = GreeksEngine()
