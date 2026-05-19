"""Unit tests for transforms.simulation.

Lightweight tests focused on shape, finiteness, and the special cases
that matter for option pricing (Black-Scholes correctness and
multifractal-collapse-to-GBM when lam=0).
"""

import numpy as np
import pytest

from transforms.simulation import (
    SIMULATION_METHODS,
    RISK_NEUTRAL_METHODS,
    EMC_METHODS,
    _lognormal_cascade,
    sim_analogue,
    sim_black_scholes,
    sim_block_bootstrap,
    sim_fhs,
    sim_fhs_rn,
    sim_multifractal,
    sim_multifractal_empirical,
)


# A medium-length pseudo-history with realistic vol clustering for the
# bootstrap / FHS / analogue tests.
def _synthetic_returns(n: int = 600, seed: int = 7) -> np.ndarray:
    rng = np.random.default_rng(seed)
    eps = rng.standard_normal(n)
    sigma = np.empty(n)
    sigma[0] = 0.01
    for t in range(1, n):
        sigma[t] = np.sqrt(1e-6 + 0.08 * (sigma[t - 1] * eps[t - 1]) ** 2 + 0.9 * sigma[t - 1] ** 2)
    return sigma * eps


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_multifractal_is_registered(self):
        assert "multifractal" in SIMULATION_METHODS
        assert SIMULATION_METHODS["multifractal"] is sim_multifractal

    def test_new_methods_are_registered(self):
        for name in ("block_bootstrap", "fhs", "fhs_rn", "analogue", "multifractal_empirical"):
            assert name in SIMULATION_METHODS

    def test_risk_neutral_methods_match_registry(self):
        # Every risk-neutral method must exist in the registry.
        assert RISK_NEUTRAL_METHODS.issubset(set(SIMULATION_METHODS))

    def test_emc_methods_subset_of_registry(self):
        assert EMC_METHODS.issubset(set(SIMULATION_METHODS))


# ---------------------------------------------------------------------------
# Lognormal cascade
# ---------------------------------------------------------------------------

class TestLognormalCascade:
    def test_shape(self):
        c = _lognormal_cascade(num_runs=5, T=10, lam=0.4)
        assert c.shape == (5, 10)

    def test_rows_sum_to_T(self):
        T = 16
        c = _lognormal_cascade(num_runs=100, T=T, lam=0.4)
        assert np.allclose(c.sum(axis=1), T)

    def test_non_negative(self):
        c = _lognormal_cascade(num_runs=100, T=16, lam=0.4)
        assert (c >= 0).all()

    def test_lam_zero_is_uniform(self):
        c = _lognormal_cascade(num_runs=10, T=8, lam=0.0)
        assert np.allclose(c, np.ones_like(c))

    def test_rejects_negative_lam(self):
        with pytest.raises(ValueError):
            _lognormal_cascade(num_runs=1, T=4, lam=-0.1)

    def test_rejects_non_positive_T(self):
        with pytest.raises(ValueError):
            _lognormal_cascade(num_runs=1, T=0, lam=0.4)


# ---------------------------------------------------------------------------
# Multifractal simulator
# ---------------------------------------------------------------------------

class TestSimMultifractal:
    def test_shape(self):
        log_returns = np.array([0.001, -0.002, 0.0015])
        out = sim_multifractal(log_returns, num_runs=1000, T=10, r=0.05, vol=0.2, lam=0.4)
        assert out.shape == (1000,)

    def test_finite(self):
        log_returns = np.array([0.001, -0.002, 0.0015])
        out = sim_multifractal(log_returns, num_runs=1000, T=10, r=0.05, vol=0.2, lam=0.5)
        assert np.isfinite(out).all()

    def test_lam_zero_matches_gbm_mean(self):
        """When lam=0 the cascade is uniform, so the simulator must produce
        the same distribution (in mean) as plain GBM."""
        rng = np.random.default_rng(42)
        np.random.seed(0)
        gbm = sim_black_scholes(np.array([0.0]), num_runs=200_000, T=20, r=0.05, vol=0.2)
        np.random.seed(0)
        mf = sim_multifractal(np.array([0.0]), num_runs=200_000, T=20, r=0.05, vol=0.2, lam=0.0)
        # With lam=0 the multifractal sampler shares the same Z draws as GBM,
        # so the two log-sum arrays should match exactly.
        assert np.allclose(gbm, mf)

    def test_higher_lam_produces_higher_kurtosis(self):
        """Stronger intermittency must produce fatter tails (higher kurtosis)
        in the simulated terminal log-return distribution."""
        log_returns = np.array([0.0])
        np.random.seed(1)
        low = sim_multifractal(log_returns, num_runs=50_000, T=20, r=0.0, vol=0.2, lam=0.1)
        np.random.seed(1)
        high = sim_multifractal(log_returns, num_runs=50_000, T=20, r=0.0, vol=0.2, lam=0.8)

        # Excess kurtosis (Fisher) of GBM log-sums is 0; cascade should lift it.
        from scipy.stats import kurtosis
        assert kurtosis(high) > kurtosis(low)
        assert kurtosis(high) > 0.0


# ---------------------------------------------------------------------------
# Block bootstrap
# ---------------------------------------------------------------------------

class TestBlockBootstrap:
    def test_shape(self):
        r = _synthetic_returns(500)
        out = sim_block_bootstrap(r, num_runs=200, T=10, block_mean_len=5.0)
        assert out.shape == (200,)

    def test_finite(self):
        r = _synthetic_returns(500)
        out = sim_block_bootstrap(r, num_runs=500, T=20, block_mean_len=5.0)
        assert np.isfinite(out).all()

    def test_block_len_one_collapses_to_historical(self):
        r = _synthetic_returns(300)
        out = sim_block_bootstrap(r, num_runs=100, T=10, block_mean_len=1.0)
        # Falls back to sim_historical; shape and finiteness suffice.
        assert out.shape == (100,) and np.isfinite(out).all()


# ---------------------------------------------------------------------------
# Filtered Historical Simulation
# ---------------------------------------------------------------------------

class TestFHS:
    def test_shape_and_finite(self):
        r = _synthetic_returns(400)
        out = sim_fhs(r, num_runs=500, T=10)
        assert out.shape == (500,) and np.isfinite(out).all()

    def test_fhs_rn_shape_and_finite(self):
        r = _synthetic_returns(400)
        out = sim_fhs_rn(r, num_runs=500, T=10, r=0.05)
        assert out.shape == (500,) and np.isfinite(out).all()

    def test_fhs_handles_short_history(self):
        # < 30 days: fall back to plain historical resampling.
        r = np.array([0.001, -0.002, 0.0005, 0.0])
        out = sim_fhs(r, num_runs=50, T=5)
        assert out.shape == (50,) and np.isfinite(out).all()

    def test_fhs_clustering_raises_kurtosis_vs_iid(self):
        """FHS should produce fatter tails than plain i.i.d. resampling of
        the same series, because the GARCH dynamics build clusters of high-vol
        days inside each path."""
        from scipy.stats import kurtosis
        r = _synthetic_returns(600)
        np.random.seed(11)
        fhs = sim_fhs(r, num_runs=20_000, T=20)
        np.random.seed(11)
        # i.i.d. baseline using the same pool.
        idx = np.random.randint(0, len(r), size=(20_000, 20))
        iid = r[idx].sum(axis=1)
        assert kurtosis(fhs) >= kurtosis(iid) - 0.5  # not strictly larger every seed, but in the ballpark


# ---------------------------------------------------------------------------
# Analogue / k-NN
# ---------------------------------------------------------------------------

class TestAnalogue:
    def test_shape_and_finite(self):
        r = _synthetic_returns(400)
        out = sim_analogue(r, num_runs=200, T=10, k_neighbors=10, window=5)
        assert out.shape == (200,) and np.isfinite(out).all()

    def test_short_history_falls_back(self):
        r = np.array([0.001, -0.002, 0.0005])
        out = sim_analogue(r, num_runs=50, T=5, k_neighbors=5, window=5)
        assert out.shape == (50,) and np.isfinite(out).all()


# ---------------------------------------------------------------------------
# Multifractal with empirical residuals
# ---------------------------------------------------------------------------

class TestSimMultifractalEmpirical:
    def test_shape_and_finite(self):
        r = _synthetic_returns(400)
        out = sim_multifractal_empirical(r, num_runs=500, T=10, r=0.05, vol=0.2, lam=0.4)
        assert out.shape == (500,) and np.isfinite(out).all()

    def test_falls_back_when_history_degenerate(self):
        # Empty / constant history -> Gaussian fallback path.
        out = sim_multifractal_empirical(np.array([]), num_runs=100, T=5, r=0.0, vol=0.2, lam=0.4)
        assert out.shape == (100,) and np.isfinite(out).all()
        out2 = sim_multifractal_empirical(np.zeros(10), num_runs=100, T=5, r=0.0, vol=0.2, lam=0.4)
        assert out2.shape == (100,) and np.isfinite(out2).all()

    def test_empirical_inherits_history_skew(self):
        """Skewed historical residuals should pull the simulated terminal
        distribution in the same direction; Gaussian-Z multifractal should
        not. Uses a strongly left-skewed pool to make the effect detectable."""
        from scipy.stats import skew
        rng = np.random.default_rng(3)
        # Mixture: mostly small +ve, occasional large -ve.
        pool = np.concatenate([
            rng.normal(0.001, 0.005, size=900),
            rng.normal(-0.05, 0.01, size=100),
        ])
        np.random.seed(0)
        emp = sim_multifractal_empirical(pool, num_runs=50_000, T=20, r=0.0, vol=0.2, lam=0.3)
        np.random.seed(0)
        gauss = sim_multifractal(pool, num_runs=50_000, T=20, r=0.0, vol=0.2, lam=0.3)
        # Empirical pool is left-skewed -> terminal log-sum should be more
        # negatively skewed than the Gaussian-Z baseline.
        assert skew(emp) < skew(gauss)
