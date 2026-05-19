# Empirical-distribution Monte Carlo methods

This note explains the four new simulation methods added alongside `multifractal`:

- `block_bootstrap` — Politis–Romano stationary bootstrap
- `fhs` — Filtered Historical Simulation (P-measure)
- `fhs_rn` — Filtered Historical Simulation (Q-measure, EMC-corrected, discounted)
- `analogue` — k-NN state-conditional bootstrap

and the cross-cutting **Empirical Martingale Correction (EMC)** post-processing
step that can be applied to any of the existing non-risk-neutral methods via
`run_simulations(extra_emc_methods=…)`.

The motivation, in one sentence: the existing `window` method beats
`black_scholes` on puts because it accidentally captures real-world **fat
tails** and **volatility clustering**, but pays for it with look-back bias and
a missing risk-neutral interpretation. These four methods attack those two
problems directly and with tunable knobs.

## 1. Why GBM under-prices puts

Black–Scholes assumes daily log-returns are i.i.d. Gaussian with a single
constant σ. Real equity returns violate both halves of that assumption:

1. **Leptokurtic distribution.** Extreme down-days are far more frequent
   than a Gaussian with the historical σ would predict.
2. **Volatility clustering.** A big move today makes a big move tomorrow
   more likely; vol arrives in bursts.

Puts are convex in the left tail, so under-priced left-tail mass under-prices
puts. The `window` method accidentally captures both by sampling a single
contiguous block of real history — but at the cost of look-back bias (only
one episode per path) and a non-risk-neutral interpretation.

## 2. Block bootstrap (`sim_block_bootstrap`)

The Politis–Romano (1994) **stationary bootstrap** is a simple,
parameter-free upgrade to `historical`. Instead of sampling individual days
i.i.d., it concatenates consecutive **blocks** drawn from history. Block
lengths are i.i.d. Geometric(1 / `block_mean_len`), block starts are i.i.d.
Uniform[0, n) with wrap-around.

This preserves short-range autocorrelation and the local clustering that
i.i.d. resampling destroys. Compared to `window`, each path now contains many
clustered sub-episodes rather than a single contiguous run, which reduces
look-back bias without losing the dependence structure.

**One parameter:** `block_mean_len` (default 5 days). At 1.0 it collapses to
`sim_historical`; for very long values it approaches `sim_window`.

## 3. Filtered Historical Simulation (`sim_fhs`, `sim_fhs_rn`)

FHS (Barone-Adesi, Engle & Mancini 2008) is the bootstrap version of GARCH
and the standard tool used by banks for short-horizon VaR. It is the most
important of the four methods.

### Construction

1. Fit a GARCH(1,1) by MLE on historical log-returns:
   $$\sigma_t^2 = \omega + \alpha\,\varepsilon_{t-1}^2 + \beta\,\sigma_{t-1}^2$$
2. Compute **standardised residuals** $z_t = \varepsilon_t / \hat\sigma_t$.
   These are approximately i.i.d. (the GARCH has soaked up the clustering)
   but still empirically **fat-tailed** — peaked centre, heavy tails — which
   is exactly what GBM is missing.
3. Roll the variance recursion forward T days, **sampling a fresh $z$ from
   the empirical residual pool** at every step:
   $$r_{T+i} = \mu_i + \sigma_{T+i}\, z^{*}_i,
     \qquad \sigma_{T+i+1}^2 = \omega + \alpha\,(\sigma_{T+i} z^{*}_i)^2 + \beta\,\sigma_{T+i}^2.$$

The drift $\mu_i$ depends on the measure:

- `sim_fhs` (P-measure, undiscounted): $\mu_i = \bar r$ (sample mean of history).
- `sim_fhs_rn` (Q-measure, EMC-corrected, discounted):
  $\mu_i = r/252 - \tfrac{1}{2}\sigma_{T+i}^2$.

### Why FHS matters

Distinctly from every other method in the registry:

- **Fat tails for free** — you are literally resampling real shocks.
- **Clustering for free** — a big sampled shock raises the next day's
  variance forecast organically through the GARCH recursion.
- **Regime-aware** — the path starts from today's conditional vol, not the
  long-run average. `window` and `historical` are unconditional and cannot
  do this.
- **Two MLE-fit knobs** (α, β) instead of one hand-picked parameter (`lam`
  in `multifractal`).

`sim_fhs_rn` is the new flagship risk-neutral pricer: combine FHS's
clustering and regime-awareness with the EMC martingale correction and a
proper PV discount, and you get a no-arbitrage-consistent option price built
from empirical residuals.

## 4. Analogue / k-NN bootstrap (`sim_analogue`)

A different angle on conditioning: instead of conditioning on σ via GARCH,
condition on the **recent return state**.

For each historical day $t$ build a 2-D feature vector $(r^{(5)}_t,
\sigma^{(5)}_t)$ — the rolling 5-day return sum and 5-day return std. At
simulation step $i$, every path computes the same features from its own last
5 simulated returns, queries a KD-tree for the $k$ historically nearest
matches, and samples one of their actual next-day returns uniformly.

This is a non-parametric Markov simulator (Paparoditis & Politis 2002). It
asks: "what tends to follow days that look like today?" It's most useful for
very short horizons (1–5 days) and for tickers with autoregressive patterns
(post-shock mean reversion, vol echoes) that GARCH does not capture.

**Two parameters:** `k_neighbors` (default 20) and `window` (default 5). Both
are robust over a wide range; only `window` materially changes behaviour.

## 5. Empirical Martingale Correction (EMC)

Duan & Simonato (1998). One line of code, applied as a post-processing
step in `run_simulations`.

### The problem it solves

Risk-neutral pricing requires
$\mathbb{E}^Q[S_T] = S_0\,e^{rT}$.
For any simulator that doesn't naturally produce this — i.e. all the
empirical-distribution methods — the sample forward price is biased, so the
resulting option prices are arbitrage-able.

### The fix

For each path, rescale the terminal price by the **same constant** that
forces the sample mean to its risk-neutral target:

$$
\tilde S_T^{(i)} = S_T^{(i)} \cdot \frac{S_0\,e^{rT/252}}{\bar S_T},
\qquad \bar S_T = \frac{1}{N}\sum_i S_T^{(i)}.
$$

By construction the corrected sample is a discrete martingale. The shape of
the distribution (its higher moments, its tails, its clustering) is
preserved — only the scale is adjusted.

### Wiring in the codebase

- `EMC_METHODS = {"fhs_rn"}` — the default set; `sim_fhs_rn` is corrected
  out of the box.
- `RISK_NEUTRAL_METHODS = {"black_scholes", "multifractal", "fhs_rn"}` —
  these also get the standard $e^{-rT/252}$ discount applied to expected
  payoffs.
- `run_simulations(..., extra_emc_methods={"historical", "window"})` — opt
  any other method into EMC for direct pre- vs post-correction comparison.

## 6. How to use these distinctly from the existing methods

The five existing + four new + EMC pieces cover a clean 2×2 plus an
orthogonal axis:

|                              | Unconditional sample             | Conditional / clustered      |
| ---------------------------- | -------------------------------- | ---------------------------- |
| **Empirical residuals**      | `historical`, `window`, `block_bootstrap` | `fhs`, `analogue`, `multifractal_empirical` |
| **Parametric residuals**     | `black_scholes`, `student_t`     | `multifractal`               |

EMC sits orthogonal: applied to any method in the left column, it turns
that method into a discrete martingale and makes it apples-to-apples
comparable to `black_scholes`.

### Suggested experiments

1. **Pre- vs post-EMC for `historical` / `window`** — run
   `run_simulations(..., extra_emc_methods={"historical", "window",
   "block_bootstrap"})` and read off the MAPE delta in
   `mc_vs_actual_test.ipynb`. Quantifies the bias EMC removes.
2. **`fhs_rn` vs `black_scholes` vs `multifractal`** — three risk-neutral
   pricers with very different sources of fat tails (residual pool / GARCH
   recursion / cascade). The disagreement on OTM puts is informative.
3. **`block_bootstrap(block_mean_len)` sweep** — at 1.0 it is `historical`,
   at very large values it is `window`; everything in between interpolates.
4. **`analogue(window, k_neighbors)` for very short horizons (T ≤ 5)** —
   most likely to pay off on weekly options.

## 7. Cost

All four methods are pure-NumPy and vectorised across paths:

| Method            | Per-step cost            | Total cost (T days, N paths)               |
| ----------------- | ------------------------ | ------------------------------------------ |
| `block_bootstrap` | O(N) (column loop)       | O(NT); ~1.5× `sim_historical`              |
| `fhs`, `fhs_rn`   | O(N) + GARCH MLE (once)  | O(NT) + ~50 ms fit; ~2× `sim_black_scholes` |
| `analogue`        | O(N log n) KD-tree query | O(NT log n); slowest of the four            |

All comfortably within the documented 10 M paths / 25 s envelope for the
existing methods on Standard\_D8ds\_v4.

## 8. Tests

Lightweight unit tests live in
[tests/test_simulation.py](../src/tests/test_simulation.py) and cover, for
each new method:

- Output shape and finiteness.
- Degenerate / short-history fallbacks (`block_mean_len=1` collapses to
  i.i.d., FHS / analogue fall back on <30 days of history).
- FHS produces at least as much kurtosis as plain i.i.d. resampling of the
  same series.

## 9. References

- Politis, D. N., & Romano, J. P. (1994). *The Stationary Bootstrap*. JASA.
- Paparoditis, E., & Politis, D. N. (2002). *The Local Bootstrap for Markov
  Processes*. Journal of Statistical Planning and Inference.
- Bollerslev, T. (1986). *Generalized Autoregressive Conditional
  Heteroskedasticity*. Journal of Econometrics. — the GARCH(1,1) used
  inside `sim_fhs`.
- Barone-Adesi, G., Engle, R. F., & Mancini, L. (2008). *A GARCH Option
  Pricing Model with Filtered Historical Simulation*. Review of Financial
  Studies. — the canonical reference for FHS option pricing.
- Duan, J.-C., & Simonato, J.-G. (1998). *Empirical Martingalisation of
  Asset Returns*. Management Science. — the EMC post-correction.
- Hull, J. C. *Options, Futures, and Other Derivatives*, ch. 22 (Value at
  Risk / Historical Simulation) for textbook treatment.
