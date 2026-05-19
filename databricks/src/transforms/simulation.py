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
from scipy.optimize import minimize
from scipy.spatial import cKDTree


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


def _lognormal_cascade(num_runs: int, T: int, lam: float, levels: int | None = None) -> np.ndarray:
    """Generate a lognormal multiplicative cascade of trading-time increments.

    Implements Mandelbrot's Multifractal Model of Asset Returns (MMAR) time
    deformation: starting from a uniform [0, T] interval, recursively split
    each cell into two halves and reweight by independent Lognormal(−½λ², λ²)
    multipliers (so E[W] = 1). After L = ceil(log2(T)) levels the cascade is
    cropped to T leaves and rescaled so each row sums to T, preserving the
    total amount of "trading time" per path.

    Args:
        num_runs: Number of independent paths.
        T: Number of trading-day leaves to return per path.
        lam: Intermittency parameter (lambda). lam=0 -> uniform time -> plain GBM.
        levels: Cascade depth. Defaults to ceil(log2(max(T, 2))).

    Returns:
        Array of shape (num_runs, T) of non-negative trading-time increments
        whose rows sum to T.
    """
    if T <= 0:
        raise ValueError(f"T must be positive, got {T}")
    if lam < 0:
        raise ValueError(f"lam must be >= 0, got {lam}")
    if lam == 0.0:
        # Degenerate cascade collapses to uniform increments.
        return np.ones((num_runs, T))

    L = levels if levels is not None else int(np.ceil(np.log2(max(T, 2))))
    n_leaves = 1 << L  # 2 ** L
    mu_w = -0.5 * lam * lam

    cascade = np.ones((num_runs, n_leaves), dtype=np.float64)
    for l in range(L):
        # At level l there are 2**(l+1) independent cells, each spanning
        # n_leaves / 2**(l+1) leaves. One Lognormal multiplier per cell.
        n_cells = 1 << (l + 1)
        block = n_leaves // n_cells
        w_l = np.random.lognormal(mean=mu_w, sigma=lam, size=(num_runs, n_cells))
        cascade *= np.repeat(w_l, block, axis=1)

    # Crop to T leaves and renormalise rows so the trading-time budget is preserved.
    cascade = cascade[:, :T]
    row_sums = cascade.sum(axis=1, keepdims=True)
    # Guard against the (vanishingly unlikely) all-zero row.
    row_sums = np.where(row_sums > 0, row_sums, 1.0)
    cascade *= (T / row_sums)
    return cascade


def sim_multifractal(
    log_returns: np.ndarray, num_runs: int, T: int,
    r: float = 0.0, vol: float = 0.2, lam: float = 0.4,
    cascade_levels: int | None = None, **kwargs,
) -> np.ndarray:
    """Mandelbrot Multifractal "baby" model: GBM subordinated to multifractal time.

    This is the Mandelbrot/Fisher/Calvet MMAR construction: keep Brownian
    motion as the engine, but feed it a non-linear randomised "trading time"
    Θ(t) built from a lognormal multiplicative cascade. The result is a
    risk-neutral price process that reproduces both empirical fat tails and
    volatility clustering, controlled by a single parameter (`lam`).

    Per path::

        delta_lnS_i = (r - 0.5*vol^2) * dΘ_i + vol * sqrt(dΘ_i) * Z_i

    with (dΘ_1, ..., dΘ_T) drawn from the cascade and summing to T. The
    total expected variance vol^2 * T is therefore preserved; the cascade only
    redistributes it across the path.

    Args:
        log_returns: Not used directly (included for API consistency).
        num_runs: Number of Monte Carlo paths.
        T: Number of trading days to simulate.
        r: Annualized risk-free interest rate.
        vol: Annualized volatility (unconditional).
        lam: Intermittency parameter for the lognormal cascade. Larger values
            produce more clustering and fatter tails. lam=0 recovers GBM.
        cascade_levels: Optional override for cascade depth.

    Returns:
        Array of log-sums (cumulative log-returns over T days), shape (num_runs,).
    """
    dt = 1.0 / 252.0
    theta = _lognormal_cascade(num_runs, T, lam=lam, levels=cascade_levels)

    Z = np.random.normal(size=(num_runs, T))
    drift = (r - 0.5 * vol * vol) * dt * theta
    diffusion = vol * np.sqrt(dt * theta) * Z
    return (drift + diffusion).sum(axis=1)


def sim_multifractal_empirical(
    log_returns: np.ndarray, num_runs: int, T: int,
    r: float = 0.0, vol: float = 0.2, lam: float = 0.4,
    cascade_levels: int | None = None, **kwargs,
) -> np.ndarray:
    """MMAR with empirical (non-Gaussian) residuals.

    Variant of `sim_multifractal` that replaces the standard-normal shocks Z
    with the empirical standardised residuals of the historical log-return
    series. The cascade still supplies the trading-time deformation (fat
    tails + clustering), while the residual pool supplies any additional
    non-Gaussian shape (skew, extra leptokurtosis) that is present in the
    raw data but not captured by a Gaussian mixture.

    Per path::

        delta_lnS_i = (r - 0.5*vol^2) * d\u0398_i + vol * sqrt(d\u0398_i) * z*_i

    where z*_i is sampled with replacement from
    (log_returns - mean(log_returns)) / std(log_returns).

    This is no longer strictly MMAR (which is defined with Brownian motion
    in multifractal time) but is closer in spirit to FHS-with-multifractal-
    variance. It is risk-neutral and discounted in the same way as
    `sim_multifractal`.

    Args:
        log_returns: 1D array of historical log returns (used to build the
            empirical residual pool). Falls back to Gaussian shocks if fewer
            than 2 finite, non-zero-std values are available.
        num_runs: Number of Monte Carlo paths.
        T: Number of trading days to simulate.
        r: Annualized risk-free interest rate.
        vol: Annualized volatility (unconditional).
        lam: Intermittency parameter for the lognormal cascade.
        cascade_levels: Optional override for cascade depth.

    Returns:
        Array of log-sums (cumulative log-returns over T days), shape (num_runs,).
    """
    dt = 1.0 / 252.0
    theta = _lognormal_cascade(num_runs, T, lam=lam, levels=cascade_levels)

    arr = np.asarray(log_returns, dtype=np.float64).ravel()
    arr = arr[np.isfinite(arr)]
    std = float(arr.std()) if arr.size >= 2 else 0.0
    if arr.size < 2 or std == 0.0:
        # Degenerate empirical pool -> fall back to Gaussian shocks so the
        # method still produces a valid simulation.
        Z = np.random.normal(size=(num_runs, T))
    else:
        z_pool = (arr - arr.mean()) / std
        idx = np.random.randint(0, z_pool.size, size=(num_runs, T))
        Z = z_pool[idx]

    drift = (r - 0.5 * vol * vol) * dt * theta
    diffusion = vol * np.sqrt(dt * theta) * Z
    return (drift + diffusion).sum(axis=1)


# ---------------------------------------------------------------------------
# Block bootstrap (Politis-Romano stationary bootstrap)
# ---------------------------------------------------------------------------

def sim_block_bootstrap(
    log_returns: np.ndarray, num_runs: int, T: int,
    block_mean_len: float = 5.0, **kwargs,
) -> np.ndarray:
    """Stationary block bootstrap of historical log returns.

    Each path is built by concatenating consecutive blocks drawn from
    historical returns. Block lengths are i.i.d. Geometric(1 / block_mean_len),
    block start indices are i.i.d. Uniform[0, n) with wrap-around. This
    preserves short-range autocorrelation and volatility clustering that
    pure i.i.d. resampling (`sim_historical`) destroys, while drawing many
    sub-windows per path (unlike `sim_window` which uses a single contiguous
    window). Politis & Romano (1994).

    Args:
        log_returns: 1D array of historical log returns.
        num_runs: Number of Monte Carlo paths.
        T: Number of trading days per path.
        block_mean_len: Expected block length (days). Smaller -> closer to
            i.i.d. resampling; larger -> closer to a single contiguous window.

    Returns:
        Array of log-sums, shape (num_runs,).
    """
    if block_mean_len <= 1.0:
        # Degenerate -> just i.i.d. resampling.
        return sim_historical(log_returns, num_runs, T)

    n = len(log_returns)
    p = 1.0 / block_mean_len  # geometric success probability

    # Pre-decide whether each cell in the (num_runs, T) grid starts a new
    # block. A new block starts at column 0 always; subsequent columns start
    # a new block with probability p (geometric block lengths).
    new_block = np.random.random(size=(num_runs, T)) < p
    new_block[:, 0] = True

    # Per-path running index into the historical array. We update it lazily:
    # at each "new block" cell, pick a fresh random start; otherwise increment.
    starts = np.random.randint(0, n, size=(num_runs, T))

    # Build the index matrix column-by-column. T is small (typically 5-30),
    # so this loop is cheap and stays vectorised across paths.
    indices = np.empty((num_runs, T), dtype=np.int64)
    indices[:, 0] = starts[:, 0]
    for j in range(1, T):
        prev = indices[:, j - 1]
        indices[:, j] = np.where(new_block[:, j], starts[:, j], (prev + 1) % n)

    # Wrap any propagated indices (only matters if prev+1 exceeded n).
    indices %= n
    sampled = log_returns[indices]
    return sampled.sum(axis=1)


# ---------------------------------------------------------------------------
# Filtered Historical Simulation (Barone-Adesi, Engle, Mancini 2008)
# ---------------------------------------------------------------------------

def _fit_garch11(returns: np.ndarray) -> tuple[float, float, float, np.ndarray]:
    """Maximum-likelihood fit of a GARCH(1,1) to demeaned daily returns.

    Model::

        eps_t  = returns_t - mean(returns)
        sigma2_t = omega + alpha * eps_{t-1}^2 + beta * sigma2_{t-1}

    Returns:
        (omega, alpha, beta, sigma2_series). sigma2_series has the same
        length as `returns` and gives the filtered conditional variance
        at each historical day. The last entry is the conditional variance
        for the next (unseen) day.
    """
    eps = returns - np.mean(returns)
    eps2 = eps * eps
    n = len(eps)
    var0 = float(np.var(eps)) if n > 1 else 1e-6
    var0 = max(var0, 1e-12)

    def neg_loglik(params: np.ndarray) -> float:
        omega, alpha, beta = params
        if omega <= 0 or alpha < 0 or beta < 0 or alpha + beta >= 0.999:
            return 1e10
        sigma2 = np.empty(n)
        sigma2[0] = var0
        for t in range(1, n):
            sigma2[t] = omega + alpha * eps2[t - 1] + beta * sigma2[t - 1]
        # Guard against numerical underflow.
        sigma2 = np.maximum(sigma2, 1e-16)
        return 0.5 * float(np.sum(np.log(sigma2) + eps2 / sigma2))

    # RiskMetrics-flavoured init: alpha + beta ~ 0.99, omega small.
    x0 = np.array([var0 * 0.01, 0.05, 0.94])
    bounds = [(1e-12, None), (0.0, 0.999), (0.0, 0.999)]

    try:
        result = minimize(
            neg_loglik, x0, method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 200, "ftol": 1e-8},
        )
        omega, alpha, beta = (float(v) for v in result.x)
        if alpha + beta >= 0.999:
            beta = 0.999 - alpha - 1e-4
    except Exception:
        # Fall back to RiskMetrics EWMA (lambda=0.94).
        omega, alpha, beta = 1e-8, 0.06, 0.94

    # Re-filter with the fitted parameters to get the variance series.
    sigma2 = np.empty(n)
    sigma2[0] = var0
    for t in range(1, n):
        sigma2[t] = omega + alpha * eps2[t - 1] + beta * sigma2[t - 1]
    sigma2 = np.maximum(sigma2, 1e-16)
    return omega, alpha, beta, sigma2


def _fhs_paths(
    log_returns: np.ndarray, num_runs: int, T: int,
    drift_mode: str, r: float,
) -> np.ndarray:
    """Common FHS path simulator. Returns the (num_runs, T) daily log-return matrix.

    drift_mode: "empirical" -> use sample mean of returns (P-measure).
                "risk_neutral" -> use r/252 - 0.5*sigma_t^2 (Q-measure).
    """
    n = len(log_returns)
    if n < 30:
        # Not enough data to fit a meaningful GARCH; fall back to plain
        # historical resampling with no clustering structure.
        idx = np.random.randint(0, n, size=(num_runs, T))
        return log_returns[idx]

    mu = float(np.mean(log_returns))
    omega, alpha, beta, sigma2_hist = _fit_garch11(log_returns)

    # Standardised residuals (the empirical "shock" pool).
    eps_hist = log_returns - mu
    z_pool = eps_hist / np.sqrt(sigma2_hist)
    # Recentre to mean 0 so EMC and RN drift behave predictably.
    z_pool = z_pool - np.mean(z_pool)

    # One-step-ahead conditional variance forecast (the starting point).
    sigma2_next = omega + alpha * eps_hist[-1] ** 2 + beta * sigma2_hist[-1]
    sigma2_t = np.full(num_runs, sigma2_next)

    daily = np.empty((num_runs, T))
    for i in range(T):
        sigma_t = np.sqrt(sigma2_t)
        # Sample one standardized residual per path.
        z_idx = np.random.randint(0, len(z_pool), size=num_runs)
        z = z_pool[z_idx]
        shock = sigma_t * z

        if drift_mode == "risk_neutral":
            # Risk-neutral daily drift in log-space: r/252 - 0.5*sigma^2.
            r_i = (r / 252.0) - 0.5 * sigma2_t + shock
        else:
            r_i = mu + shock

        daily[:, i] = r_i
        # GARCH recursion uses the demeaned shock (variance of return, not
        # of log-return-minus-drift; the two coincide for small drift).
        sigma2_t = omega + alpha * shock ** 2 + beta * sigma2_t

    return daily


def sim_fhs(
    log_returns: np.ndarray, num_runs: int, T: int, **kwargs,
) -> np.ndarray:
    """Filtered Historical Simulation under the physical measure (undiscounted).

    Fits GARCH(1,1) to historical returns, extracts standardised residuals,
    and forward-simulates T-day paths by sampling residuals and rolling the
    GARCH variance recursion. Produces both fat tails (from the empirical
    residual pool) and volatility clustering (from the GARCH dynamics),
    starting from today's conditional vol regime. Barone-Adesi, Engle &
    Mancini (2008).
    """
    daily = _fhs_paths(log_returns, num_runs, T, drift_mode="empirical", r=0.0)
    return daily.sum(axis=1)


def sim_fhs_rn(
    log_returns: np.ndarray, num_runs: int, T: int,
    r: float = 0.0, **kwargs,
) -> np.ndarray:
    """FHS under the risk-neutral measure (discounted + EMC-corrected).

    Same simulator as `sim_fhs` but the daily drift is the risk-neutral
    drift r/252 - 0.5*sigma_t^2. Combined with the EMC post-correction
    (see EMC_METHODS) the simulated terminal prices form a discrete
    martingale, and the resulting option prices are discounted to PV.
    """
    daily = _fhs_paths(log_returns, num_runs, T, drift_mode="risk_neutral", r=r)
    return daily.sum(axis=1)


# ---------------------------------------------------------------------------
# Analogue / k-NN nearest-neighbour bootstrap
# ---------------------------------------------------------------------------

def _build_analogue_state(
    log_returns: np.ndarray, window: int = 5,
) -> tuple[cKDTree, np.ndarray, np.ndarray]:
    """Pre-compute feature vectors for each historical day.

    Feature: (sum of last `window` returns, std of last `window` returns).
    Returns the KD-tree, the per-feature standardisation (mean, std), and
    the array of "next-day" returns aligned with the feature rows.
    """
    n = len(log_returns)
    if n <= window + 1:
        raise ValueError("Not enough history to build analogue features")

    # Rolling sum and std over `window` days, aligned to day t (inclusive).
    feat_sum = np.array([log_returns[t - window + 1 : t + 1].sum() for t in range(window - 1, n - 1)])
    feat_std = np.array([log_returns[t - window + 1 : t + 1].std() for t in range(window - 1, n - 1)])
    next_ret = log_returns[window:]  # the day after each feature row

    feats = np.column_stack([feat_sum, feat_std])
    # Standardise so the two features carry comparable weight in Euclidean distance.
    mu = feats.mean(axis=0)
    sd = feats.std(axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    feats_z = (feats - mu) / sd
    tree = cKDTree(feats_z)
    return tree, np.array([mu, sd]), next_ret


def sim_analogue(
    log_returns: np.ndarray, num_runs: int, T: int,
    k_neighbors: int = 20, window: int = 5, **kwargs,
) -> np.ndarray:
    """k-NN analogue bootstrap: sample from days that historically looked like "today".

    At each simulation step every path computes a feature vector from its
    own last `window` simulated returns, queries the KD-tree for the
    `k_neighbors` historically nearest matches, and samples one of their
    actual next-day returns. This produces state-conditional (Markov)
    sampling rather than the unconditional sampling of `historical` /
    `window` / `block_bootstrap`. Paparoditis & Politis (2002).

    Args:
        log_returns: 1D array of historical log returns.
        num_runs: Number of Monte Carlo paths.
        T: Number of trading days per path.
        k_neighbors: Number of historical matches to draw from at each step.
        window: Length of the rolling feature window (days).

    Returns:
        Array of log-sums, shape (num_runs,).
    """
    n = len(log_returns)
    if n <= window + 1:
        return sim_historical(log_returns, num_runs, T)

    tree, scaling, next_ret = _build_analogue_state(log_returns, window=window)
    mu_f, sd_f = scaling[0], scaling[1]
    k = min(k_neighbors, len(next_ret))

    # Seed each path's recent-returns buffer with the last `window` historical
    # days. All paths start identical; randomness enters from the k-NN
    # tie-breaking sampling on the very first step.
    recent = np.tile(log_returns[-window:], (num_runs, 1))

    daily = np.empty((num_runs, T))
    for i in range(T):
        feat_sum = recent.sum(axis=1)
        feat_std = recent.std(axis=1)
        feats = np.column_stack([feat_sum, feat_std])
        feats_z = (feats - mu_f) / sd_f

        # Single batched KD-tree query: shape (num_runs, k) of historical row indices.
        _, idx = tree.query(feats_z, k=k)
        if k == 1:
            idx = idx[:, np.newaxis]

        # Pick one of the k neighbours per path, uniformly.
        pick = np.random.randint(0, k, size=num_runs)
        chosen = idx[np.arange(num_runs), pick]
        r_i = next_ret[chosen]

        daily[:, i] = r_i
        # Slide the buffer.
        recent = np.column_stack([recent[:, 1:], r_i])

    return daily.sum(axis=1)


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
    "multifractal": sim_multifractal,
    "multifractal_empirical": sim_multifractal_empirical,
    "block_bootstrap": sim_block_bootstrap,
    "fhs": sim_fhs,
    "fhs_rn": sim_fhs_rn,
    "analogue": sim_analogue,
}

# Methods that price under the risk-neutral measure and therefore require
# discounting of the expected payoff back to present value.
RISK_NEUTRAL_METHODS = frozenset({"black_scholes", "multifractal", "multifractal_empirical", "fhs_rn"})

# Methods to which the Empirical Martingale Correction (Duan & Simonato 1998)
# is applied by default: terminal prices are rescaled so the sample mean
# matches the risk-neutral forward S0 * exp(r*T/252) before payoff evaluation.
# Users can opt additional methods in via `run_simulations(extra_emc_methods=...)`.
EMC_METHODS = frozenset({"fhs_rn"})
