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


# ---------------------------------------------------------------------------
# Method registry
# ---------------------------------------------------------------------------

SIMULATION_METHODS = {
    "historical": sim_historical,
    "window": sim_window,
    "window_10d": sim_window_10d,
    "window_20d": sim_window_20d,
    "student_t": sim_student_t,
}
