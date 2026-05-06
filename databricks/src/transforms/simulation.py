"""Monte Carlo simulation functions for option pricing.

Pure NumPy vectorized implementations — no Spark UDFs, no broadcasts.
Each function takes the log_returns array and simulation parameters,
and returns an array of simulated log-sums (one per path).

All operations are vectorized for maximum throughput on the driver.
Typical performance: 2M paths × 10 days in <1 second.
"""

from __future__ import annotations

import numpy as np
import scipy.stats


# ---------------------------------------------------------------------------
# Simulation methods (all return np.ndarray of shape (num_runs,))
# ---------------------------------------------------------------------------

def sim_historical(log_returns: np.ndarray, num_runs: int, T: int, **kwargs) -> np.ndarray:
    """Random sampling from historical log returns (with replacement).

    Each path draws T random log returns independently.
    """
    n = len(log_returns)
    # Shape: (num_runs, T) — each row is one simulation path
    indices = np.random.randint(0, n, size=(num_runs, T))
    sampled = log_returns[indices]
    return sampled.sum(axis=1)


def sim_window(log_returns: np.ndarray, num_runs: int, T: int, **kwargs) -> np.ndarray:
    """Consecutive window: random start, take T consecutive days (wrapping)."""
    n = len(log_returns)
    starts = np.random.randint(0, n, size=num_runs)
    # Build index matrix: each row is [start, start+1, ..., start+T-1] mod n
    offsets = np.arange(T)
    indices = (starts[:, np.newaxis] + offsets) % n
    sampled = log_returns[indices]
    return sampled.sum(axis=1)


def sim_window_10d(log_returns: np.ndarray, num_runs: int, T: int, **kwargs) -> np.ndarray:
    """Window with 10-day step: random start, step by 10 days (wrapping)."""
    n = len(log_returns)
    starts = np.random.randint(0, n, size=num_runs)
    offsets = np.arange(T) * 10
    indices = (starts[:, np.newaxis] + offsets) % n
    sampled = log_returns[indices]
    return sampled.sum(axis=1)


def sim_window_20d(log_returns: np.ndarray, num_runs: int, T: int, **kwargs) -> np.ndarray:
    """Window with 20-day step: random start, step by 20 days (wrapping)."""
    n = len(log_returns)
    starts = np.random.randint(0, n, size=num_runs)
    offsets = np.arange(T) * 20
    indices = (starts[:, np.newaxis] + offsets) % n
    sampled = log_returns[indices]
    return sampled.sum(axis=1)


def sim_student_t(
    log_returns: np.ndarray, num_runs: int, T: int,
    alt_weight: float = 0.1, df: float = 3.0, tloc: float = 0.0, tscale: float = 0.01, **kwargs
) -> np.ndarray:
    """Mixed: (1-alt_weight) historical + alt_weight Student-t draws."""
    n = len(log_returns)
    # Draw all T steps for all paths from historical
    indices = np.random.randint(0, n, size=(num_runs, T))
    hist_samples = log_returns[indices]

    # Draw Student-t alternatives for all positions
    t_samples = scipy.stats.t.rvs(df=df, loc=tloc, scale=tscale, size=(num_runs, T))

    # Mask: True where we use the alternative distribution
    mask = np.random.random(size=(num_runs, T)) < alt_weight

    # Combine: use student-t where mask is True, historical otherwise
    combined = np.where(mask, t_samples, hist_samples)
    return combined.sum(axis=1)


def sim_black_scholes(
    log_returns: np.ndarray, num_runs: int, T: int,
    r: float = 0.0, vol: float = 0.2, **kwargs
) -> np.ndarray:
    """Black-Scholes GBM under risk-neutral measure (benchmark).

    Simulates geometric Brownian motion:
        dS/S = r*dt + vol*dW
    In log space:
        delta_lnS = (r - 0.5*vol²)*dt + vol*sqrt(dt)*Z

    Uses dt = 1/252 (daily trading day convention).

    Args:
        log_returns: Not used directly (included for API consistency).
        num_runs: Number of Monte Carlo paths.
        T: Number of trading days to simulate.
        r: Annualized risk-free interest rate (e.g., 0.05 for 5%).
        vol: Annualized volatility (e.g., 0.20 for 20%).

    Returns:
        Array of log-sums (cumulative log-returns over T days), shape (num_runs,).
    """
    dt = 1.0 / 252.0
    nudt = (r - 0.5 * vol**2) * dt
    volsdt = vol * np.sqrt(dt)

    # Generate standard normal draws: (num_runs, T)
    Z = np.random.normal(size=(num_runs, T))

    # Daily log-returns under GBM
    delta_lnS = nudt + volsdt * Z

    # Sum across days to get total log-return per path
    return delta_lnS.sum(axis=1)


# ---------------------------------------------------------------------------
# Method registry
# ---------------------------------------------------------------------------

SIMULATION_METHODS = {
    "historical": sim_historical,
    "window": sim_window,
    "window_10d": sim_window_10d,
    "window_20d": sim_window_20d,
    "student_t": sim_student_t,
    "black_scholes": sim_black_scholes,
}
