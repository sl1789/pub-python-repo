# Multifractal Monte Carlo for European option pricing

This note documents a 7th simulation method, `multifractal`, added to the
existing Monte Carlo pricing framework in
[databricks/src/transforms/simulation.py](../src/transforms/simulation.py).
It is motivated by Benoit Mandelbrot's *Multifractal Model of Asset Returns*
(MMAR) and answers the two empirical failings of geometric Brownian motion
that Shimabuku highlights in
[*What the Father of Fractals Can Teach Us About Finance*](https://medium.com/@jordanshimabuku/what-the-father-of-fractals-can-teach-us-about-finance-e3752fe3fc32):

1. Real return distributions are **leptokurtic** — peaked centre, fat tails.
2. Extreme days **cluster** (dot-com, GFC, COVID) — they are not i.i.d.

## Background — what the article actually argues

Shimabuku walks the reader through three models and shows where each breaks:

| Model | Captures | Fails to capture |
| --- | --- | --- |
| Discrete random walk | Unpredictability | Zig-zag artefacts, no magnitude variation |
| Geometric Brownian motion (Black–Scholes) | Drift, Gaussian noise | Fat tails, volatility clustering |
| **Multifractal Model of Asset Returns (MMAR)** | Fat tails *and* clustering | More parameters, harder to calibrate |

Mandelbrot's fix keeps Brownian motion as the diffusion engine but feeds it a
non-linear, randomised *trading time* $\theta(t)$ built from a **multiplicative
cascade**. The cascade recursively splits an interval into halves and
reweights each half by a random multiplier with mean 1. Iterating $L$ levels
yields a measure that is locally lumpy at every scale — exactly the kind of
self-similar burstiness that the VIX, intraday volume, and realised
volatility actually exhibit.

## The "baby" multifractal model

Under the risk-neutral measure, model log-returns over $T$ trading days as

$$
\ln\!\left(\frac{S_T}{S_0}\right)
  = \sum_{i=1}^{T} \left[\left(r - \tfrac{1}{2}\sigma^2\right)\Delta\theta_i
    + \sigma \sqrt{\Delta\theta_i}\, Z_i\right],
\qquad Z_i \stackrel{\text{iid}}{\sim} \mathcal{N}(0, 1),
$$

where $(\Delta\theta_1, \dots, \Delta\theta_T)$ is a random partition of
$[0, T]$ drawn from a multiplicative cascade and satisfying
$\sum_i \Delta\theta_i = T$.

Because the cascade preserves total time, the **unconditional variance**
$\sigma^2 T$ is the same as plain GBM — the cascade only redistributes that
variance across the path. This redistribution is precisely what gives the
model its two desired statistical properties:

- Conditional on $\theta$, the terminal log-return is Gaussian with random
  variance $\sigma^2 T$; integrating over the cascade produces a Gaussian
  mixture, which is leptokurtic. Fat tails emerge automatically.
- Sibling cells in the cascade tree share their high-level multipliers, so a
  "fat" sub-tree colours an entire contiguous block of days. Volatility
  clustering emerges automatically.

In Mandelbrot's terminology this is the *baby* model — the mother is Brownian
motion, the father is the multifractal time change.

### Lognormal cascade (what we implement)

For the multipliers we use independent
$W \sim \text{Lognormal}\!\big(-\tfrac{1}{2}\lambda^2,\;\lambda^2\big)$
so that $\mathbb{E}[W] = 1$. The single new tuning parameter $\lambda$ is the
**intermittency**:

- $\lambda = 0$: degenerate cascade, uniform increments, the model collapses
  to plain GBM (and the existing `sim_black_scholes` benchmark).
- Small $\lambda$ (e.g. 0.1–0.3): mild clustering, modest fat tails.
- Large $\lambda$ (e.g. 0.5–0.8): pronounced bursts; appropriate for stress
  scenarios or tickers with strong jump-like behaviour.

This is the standard MMAR construction from Mandelbrot, Fisher & Calvet
(1997) and is closer to a continuous limit than Mandelbrot's original
binomial cascade. The continuous-time analogue is the multifractal random
measure, but the discrete cascade above is exactly what is implementable in
NumPy without auxiliary processes.

## Implementation notes

The full implementation lives in
[transforms/simulation.py](../src/transforms/simulation.py) and is
vectorised in the same style as the other six methods (no loops over paths,
no Spark UDFs).

```python
def _lognormal_cascade(num_runs, T, lam, levels=None):
    # Build cascade with L = ceil(log2(T)) levels and 2**L leaves.
    # At level l there are 2**(l+1) independent multipliers; each is
    # replicated across the leaves of its sub-tree via np.repeat.
    # Crop to T leaves and renormalise so each row sums to T.
    ...

def sim_multifractal(log_returns, num_runs, T, r, vol, lam, **kwargs):
    dt = 1.0 / 252.0
    theta = _lognormal_cascade(num_runs, T, lam=lam)
    Z = np.random.normal(size=(num_runs, T))
    drift = (r - 0.5 * vol * vol) * dt * theta
    diffusion = vol * np.sqrt(dt * theta) * Z
    return (drift + diffusion).sum(axis=1)
```

### Variant: `sim_multifractal_empirical`

`sim_multifractal` uses Gaussian shocks $Z_i$, in line with the canonical
MMAR formulation "Brownian motion in multifractal time". Empirical equity
returns, however, are not just leptokurtic-via-mixing — the underlying daily
residuals are themselves fat-tailed and (especially for indices) negatively
skewed. To isolate that effect we register a sibling method:

```python
def sim_multifractal_empirical(log_returns, num_runs, T, r, vol, lam, **kwargs):
    dt = 1.0 / 252.0
    theta = _lognormal_cascade(num_runs, T, lam=lam)
    z_pool = (log_returns - log_returns.mean()) / log_returns.std()
    Z = np.random.choice(z_pool, size=(num_runs, T), replace=True)
    drift = (r - 0.5 * vol * vol) * dt * theta
    diffusion = vol * np.sqrt(dt * theta) * Z
    return (drift + diffusion).sum(axis=1)
```

This is no longer strictly MMAR but is a natural hybrid: cascade-driven
volatility clustering plus empirically fat / skewed shocks. It is still
risk-neutral (same drift) and discounted by the same factor. With a
degenerate history (n < 2 or zero std) the simulator falls back to Gaussian
shocks so it never raises.

The driver
[`run_simulations`](../src/utils/simulation_helpers.py)
adds:

- A `lam` parameter (default `0.4`) wired into the shared `sim_kwargs`.
- A `RISK_NEUTRAL_METHODS = {"black_scholes", "multifractal", "multifractal_empirical", "fhs_rn"}`
  set used to apply the $e^{-rT/252}$ discount factor to expected payoffs.

Both `sim_multifractal` and `sim_multifractal_empirical` are registered in
`SIMULATION_METHODS`, so they pick up the
full pipeline automatically — the
[`monte_carlo_simulation`](../jobs/monte_carlo_simulation.ipynb),
[`scalability_test`](../jobs/scalability_test.ipynb), and
[`mc_vs_actual_test`](../jobs/mc_vs_actual_test.ipynb) notebooks iterate the
registry and need no further changes.

## Why we discount it

The `historical`, `window*`, and `student_t` methods sample from the
*empirical* return distribution and are not risk-neutral; their option prices
are an empirical-distribution interpretation and intentionally undiscounted
(consistent with the existing codebase). Black–Scholes and the multifractal
models both price expected payoffs under the risk-neutral measure $Q$ and
therefore must be discounted by $e^{-rT/252}$ to give a present value.

## How it should perform on the existing benchmark

The `mc_vs_actual_test` notebook documents the current ranking:

| Subset | Best method | MAPE |
| --- | --- | --- |
| All strikes – calls | `black_scholes` | 46.4 % |
| All strikes – puts | `window` | 50.0 % |
| ATM – calls | `window` | 53.2 % |
| ATM – puts | `window` | 28.5 % |

`window` wins on puts because puts are sensitive to left-tail mass that GBM
under-prices; `window` accidentally captures that by recycling actual
historical sequences, but at the cost of a non-risk-neutral interpretation
and no tunable parameter.

The multifractal model should close that gap *with* a risk-neutral
interpretation:

- Right-skewed $\sigma_\theta$ produces left-tail mass in terminal prices,
  raising the price of OTM puts toward observed market levels.
- $\lambda$ is a single tunable knob, calibratable per ticker from sample
  kurtosis of historical log-returns.
- It collapses exactly to `black_scholes` at $\lambda = 0$, so the existing
  BS benchmark is a special case.

A natural next experiment is to sweep
$\lambda \in \{0.1, 0.3, 0.5, 0.7\}$ inside `mc_vs_actual_test.ipynb` and
report MAPE per ticker and per moneyness bucket.

## Cost

- Memory: $O(\text{num\_runs} \cdot T)$, same as `sim_black_scholes`.
- Time: roughly $2\times$ `sim_black_scholes` — one extra cascade construction
  and one extra elementwise multiply. Still well within the documented
  "10 M paths in 25 s on Standard\_D8ds\_v4" budget for the existing six
  methods.

## Tests

Lightweight unit tests live in
[tests/test_simulation.py](../src/tests/test_simulation.py) and cover:

- The cascade returns the correct shape, is non-negative, and renormalises
  to $\sum \Delta\theta_i = T$.
- $\lambda = 0$ collapses both the cascade and the simulator to plain GBM
  (matched arrays under shared seeds).
- Higher $\lambda$ produces higher empirical kurtosis of terminal log-returns
  than lower $\lambda$ — the headline statistical property of the model.

## References

- Mandelbrot, *The (Mis)Behavior of Markets*, chapter 11 — the
  popular-press treatment that Shimabuku draws from.
- Mandelbrot, Fisher & Calvet (1997), *A Multifractal Model of Asset
  Returns*, Cowles Foundation Discussion Paper No. 1164 — the original
  MMAR construction with the cascade definition implemented here.
- Calvet & Fisher (2001), *Forecasting Multifractal Volatility*,
  *Journal of Econometrics*.
- Calvet & Fisher (2004), *How to Forecast Long-Run Volatility: Regime
  Switching and the Estimation of Multifractal Processes*,
  *Journal of Financial Econometrics* — introduces MSM, a tractable
  Markov-switching multifractal that is MLE-estimable and consistently
  beats GARCH out of sample.
- Shimabuku, [*What the Father of Fractals Can Teach Us About Finance*](https://medium.com/@jordanshimabuku/what-the-father-of-fractals-can-teach-us-about-finance-e3752fe3fc32).
- Shimabuku follow-up, [*Implications of a Multifractal Model of Stocks on Options Pricing*](https://medium.com/@jordanshimabuku/implications-of-a-multifractal-model-of-stocks-on-options-pricing-741ca1a9eb8e).
