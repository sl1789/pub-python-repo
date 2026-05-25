"""About page: architecture, methods, and a summary of the Databricks work.

Read-only reference page. Three tabs:

  1. **Architecture**       -> high-level component diagram + responsibilities
  2. **Monte Carlo Methods** -> one-paragraph summary of every method shipped
  3. **Databricks Work**     -> per-notebook + per-doc summary with file links
"""

from __future__ import annotations

import streamlit as st

from api_client import render_session_sidebar

st.set_page_config(page_title="About - Monte Carlo Option Pricing", layout="wide")
st.title("About")
st.caption(
    "Reference material for new users: how the app is wired together, which "
    "Monte Carlo methods it implements, and what the Databricks notebooks do."
)

render_session_sidebar()


tab_arch, tab_methods, tab_dbx = st.tabs(
    ["Architecture", "Monte Carlo Methods", "Databricks Work"]
)


# ============================================================================
# Tab 1 — Architecture
# ============================================================================
with tab_arch:
    st.subheader("Component overview")
    st.markdown(
        "The app is a small job-orchestration platform on top of a Databricks "
        "compute backend. The pieces talk to each other through HTTP and a "
        "shared SQLite job table; results live as parquet on Azure ADLS Gen2."
    )

    st.code(
        """
┌──────────────┐     ┌──────────────┐     ┌────────────────────┐
│ Streamlit UI │ ──► │  FastAPI API │ ──► │  SQLite / SQLModel │
└──────────────┘     └──────┬───────┘     └────────────────────┘
                            │
                            ▼
                     ┌──────────────┐     ┌────────────────────┐
                     │  Worker loop │ ──► │ DatabricksRunner   │
                     │  (poller)    │     │ (run-now + poll)   │
                     └──────┬───────┘     └────────────────────┘
                            │
                            ▼
                     ┌──────────────────────────────────────┐
                     │ AzureParquetResultsRepository (ADLS) │
                     └──────────────────────────────────────┘
        """,
        language="text",
    )

    st.markdown("### Responsibilities at a glance")
    st.markdown(
        "- **Streamlit UI** (`ui/`) — login, submit form, jobs list, results "
        "charts, comparison page, research analysis page. Talks to the API "
        "over HTTP with a JWT bearer token.\n"
        "- **FastAPI** (`app/`) — `/auth/token`, `/jobs`, `/results`, "
        "`/datasets`, `/health`. RBAC via `viewer` / `submitter` roles, "
        "JSON error envelopes, structured logging, rate limit, security "
        "headers.\n"
        "- **SQLite (SQLModel)** (`app/db/`) — `Job`, `ResultRow`, `JobEvent`, "
        "`JobMetrics`. Schema managed by Alembic. Local file is "
        "`./app.db`; switch to Postgres by changing `DATABASE_URL`.\n"
        "- **Worker** (`worker/worker.py`) — a polling loop. Picks `QUEUED` "
        "jobs, submits them via `DatabricksRunner`, polls `RUNNING` jobs "
        "until `SUCCEEDED` / `FAILED`, writes back status + `output_ref`.\n"
        "- **DatabricksRunner** (`app/runners/databricks.py`) — wraps the "
        "Databricks Jobs 2.0 REST API: `run-now` + `runs/get`. Builds the "
        "deterministic parquet `output_ref` at submit time using "
        "`databricks/lib/paths.py` so the runner and the notebook agree on "
        "where output lands.\n"
        "- **AzureParquetResultsRepository** (`app/results/parquet_azure.py`) "
        "— reads the ticker-partitioned parquet export via `adlfs` + "
        "`pyarrow`, filters down to the `(ticker, K, T, runs)` of the "
        "requesting job, returns rows for the API."
    )

    st.markdown("### Two parallel data flows")
    st.markdown(
        "1. **MC job flow** — UI → `POST /jobs` → SQLite (`QUEUED`) → worker "
        "→ Databricks `monte_carlo_simulation.ipynb` → parquet at "
        "`<container>/<prefix>/simulations/ticker=<T>/...` → "
        "`GET /results?job_id=` → UI charts. **One job per submit.**\n"
        "2. **Research flow** — `mc_vs_actual_test.ipynb` is run **manually** "
        "in Databricks (sweeps a universe of tickers / strikes / scales) "
        "and writes four parquet datasets — `mc_vs_actual`, "
        "`emc_diagnostics`, `block_length_sweep`, `lam_sweep` — which the "
        "Analysis page reads via `/datasets/...`. **No job is created** "
        "for this flow because it's a multi-hour sweep, not a one-shot "
        "request."
    )

    st.markdown("### Security model")
    st.markdown(
        "- OAuth2 password flow at `/auth/token`, returns a JWT bearer.\n"
        "- `get_current_user` + `require_roles(...)` dependencies guard the "
        "routes; demo user is seeded from env (`DEMO_USER_*`).\n"
        "- Per-request `X-Request-ID` correlation, security headers, in-memory "
        "rate limit, JSON content-type enforcement.\n"
        "- Secrets (`JWT_SECRET`, `DATABRICKS_TOKEN`, `AZURE_STORAGE_KEY`) "
        "live in `.env` and are not committed."
    )


# ============================================================================
# Tab 2 — Monte Carlo Methods
# ============================================================================
with tab_methods:
    st.subheader("Methods implemented in `databricks/src/transforms/simulation.py`")
    st.caption(
        "Every `monte_carlo_simulation` run produces one row per method "
        "for the requested `(ticker, K, T, runs)`. The Results page lets "
        "you filter the comparison to the methods you care about."
    )

    st.markdown(
        "#### Closed-form baseline\n"
        "- **`black_scholes`** — the classic Black–Scholes–Merton analytical "
        "formula for European calls and puts. Assumes log-returns are "
        "i.i.d. Gaussian with a single constant volatility σ and continuous "
        "trading with no jumps. Closed-form, instant, zero variance. Used "
        "as the reference line in every chart; systematically under-prices "
        "out-of-the-money puts because real returns have fat left tails "
        "and clustered volatility that a single Gaussian cannot reproduce."
    )

    st.markdown(
        "#### Historical-resampling methods\n"
        "- **`historical`** — non-parametric bootstrap: sample T log-returns "
        "i.i.d. from the empirical history of the underlying, cumulate, "
        "price the payoff. Captures the empirical distribution shape (fat "
        "tails, skew) exactly but destroys all autocorrelation — every "
        "drawn day is independent, so volatility clustering disappears.\n"
        "- **`window`** — for each path, sample one contiguous window of "
        "length T from history and use those returns verbatim. Accidentally "
        "captures *both* fat tails *and* clustering because every path is "
        "a real historical episode. The trade-off is severe look-back "
        "bias (only one episode per path, often the same crisis repeating) "
        "and no risk-neutral interpretation.\n"
        "- **`window_10d` / `window_20d`** — sliding-window variants with a "
        "fixed shorter window length (10 or 20 days). Useful when T is "
        "very short and you want each path to span more than one "
        "historical episode while still preserving local clustering.\n"
        "- **`student_t`** — fit a Student-t distribution to historical "
        "log-returns by MLE (location, scale, degrees of freedom ν), then "
        "sample i.i.d. The cheap parametric way to add fat tails to GBM. "
        "Still i.i.d., so clustering is missing; ν typically lands in "
        "[3, 6] for equity indices, which matches the moneyness smile "
        "much better than Gaussian σ but leaves the term-structure smile "
        "untouched."
    )

    st.markdown(
        "#### Multifractal (Mandelbrot MMAR)\n"
        "- **`multifractal`** — keeps Brownian motion as the diffusion engine "
        "but feeds it a non-linear *trading time* $\\theta(t)$ built from a "
        "lognormal multiplicative cascade. The cascade recursively splits "
        "the interval and reweights each half by an i.i.d. random "
        "multiplier with mean 1, producing a measure that is locally "
        "lumpy at every scale — exactly the self-similar burstiness real "
        "volatility exhibits. Single tuning knob `λ` (intermittency): "
        "λ = 0 collapses to plain Black–Scholes; λ ≈ 0.2–0.3 reproduces "
        "the mild clustering of an equity index in a calm regime; "
        "λ ≈ 0.6–0.8 is appropriate for stress scenarios or assets with "
        "jump-like behaviour. Crucially, *total* variance σ²T is preserved "
        "by construction — the cascade only redistributes that variance "
        "across the path, so fat tails come from the Gaussian mixture and "
        "not from a parameter blow-up.\n"
        "- **`multifractal_empirical`** — same cascade-driven random-time "
        "change, but the Gaussian innovations $Z_i$ are replaced with "
        "samples from the empirical residual distribution of the "
        "underlying. Combines parametric clustering (from the cascade) "
        "with non-parametric tail behaviour (from the residual pool); "
        "useful when historical residuals are visibly non-Gaussian even "
        "after fitting a Student-t."
    )

    st.markdown(
        "#### Empirical (Barone-Adesi family)\n"
        "- **`block_bootstrap`** — Politis–Romano (1994) **stationary "
        "bootstrap**. Instead of sampling individual days i.i.d., "
        "concatenate consecutive *blocks* of history; block lengths are "
        "i.i.d. Geometric(1 / `block_mean_len`), starts are i.i.d. "
        "Uniform with wrap-around. Parameter-free upgrade to `historical` "
        "that preserves short-range autocorrelation and the local "
        "clustering that i.i.d. resampling destroys. One knob, "
        "`block_mean_len` (default 5 days). At `block_mean_len = 1` it "
        "collapses to `historical`; at very long values it approaches "
        "`window`.\n"
        "- **`fhs`** — **Filtered Historical Simulation** under the "
        "P-measure (Barone-Adesi, Engle & Mancini 2008). Fit a GARCH(1,1) "
        "by MLE on historical log-returns, compute standardised residuals "
        "$z_t = \\varepsilon_t / \\hat\\sigma_t$ (approximately i.i.d. but "
        "still empirically fat-tailed), then roll the variance recursion "
        "forward T days sampling fresh residuals each step. The standard "
        "bank-grade tool for short-horizon VaR. Distinctive properties: "
        "*fat tails for free* (you literally resample real shocks), "
        "*clustering for free* (a big sampled shock raises the next day's "
        "conditional vol through the GARCH recursion), and "
        "*regime-aware* — the path starts from today's σ_T, not the "
        "long-run average, so it tightens in calm regimes and widens "
        "after a shock.\n"
        "- **`fhs_rn`** — FHS under the **Q-measure** with the Empirical "
        "Martingale Correction applied and a proper PV discount. The "
        "flagship risk-neutral pricer in the registry: it combines FHS's "
        "clustering and regime-awareness with no-arbitrage consistency, "
        "so the discounted underlying is exactly a martingale under the "
        "simulated measure. The recommended default for production option "
        "pricing whenever you need a price that downstream hedging code "
        "can trust.\n"
        "- **`analogue`** — *k*-NN state-conditional bootstrap "
        "(Paparoditis & Politis 2002). For each historical day build a "
        "2-D feature vector $(r^{(5)}_t, \\sigma^{(5)}_t)$ — rolling "
        "5-day return sum and rolling 5-day return std. At each "
        "simulation step every path computes the same features from its "
        "own last 5 simulated returns, queries a KD-tree for the *k* "
        "historically nearest matches, and samples one of their actual "
        "next-day returns uniformly. Non-parametric Markov simulator "
        "that asks *“what tends to follow days that look like today?”* "
        "Most useful for very short horizons (1–5 days) and for tickers "
        "with autoregressive patterns (post-shock mean reversion, vol "
        "echoes) that GARCH cannot capture. Two knobs: `k_neighbors` "
        "(default 20) and `window` (default 5)."
    )

    st.markdown(
        "#### Cross-cutting post-processing\n"
        "- **Empirical Martingale Correction (EMC)** — Duan & Simonato "
        "(1998). A one-line additive correction applied after simulation "
        "so that the empirical mean of the discounted terminal price "
        "matches the no-arbitrage spot — i.e. the discounted underlying "
        "is exactly a martingale under the simulated measure. Cheap "
        "enough to apply to any non-risk-neutral method "
        "(`run_simulations(extra_emc_methods=…)`), and typically cuts "
        "out-of-the-money put MAPE in half. Drives the `emc_diagnostics` "
        "research dataset and the EMC pre/post tab on the Analysis page."
    )


# ============================================================================
# Tab 3 — Databricks Work
# ============================================================================
with tab_dbx:
    st.subheader("Notebooks in `databricks/jobs/`")

    st.markdown(
        "**`monte_carlo_simulation.ipynb`** — the production notebook the API "
        "submits via `DatabricksRunner`. Reads the input parameters from the "
        "Databricks job widgets, pulls the underlying price history from "
        "yfinance, runs every method in the registry once for the requested "
        "`(ticker, K, T, runs)`, and writes one parquet row per method to "
        "`<container>/<prefix>/simulations/ticker=<TICKER>/`. **Triggered "
        "from the Submit page.**"
    )

    st.markdown(
        "**`mc_vs_actual_test.ipynb`** — the research notebook behind the "
        "Analysis page. Sweeps a universe of tickers × strikes × horizons × "
        "path counts and compares each method's MC price to observed market "
        "option prices. Produces four parquet datasets:\n"
        "- `mc_vs_actual` — every (method, ticker, K, T, runs) vs the "
        "observed market option price.\n"
        "- `emc_diagnostics` — pre/post Empirical Martingale Correction at "
        "one fixed scale.\n"
        "- `block_length_sweep` — sensitivity of the block bootstrap to "
        "`block_mean_len`.\n"
        "- `lam_sweep` — sensitivity of the multifractal cascade to `λ`.\n\n"
        "**Run manually in Databricks** (takes minutes); not submittable "
        "from the UI because it is a multi-dimensional sweep, not a single "
        "job."
    )

    st.markdown(
        "**`scalability_test.ipynb`** — measures wall-clock and memory cost "
        "of each method as `num_simulations` grows. Useful for picking "
        "default path counts in the Submit presets."
    )

    st.markdown(
        "**`options_data_pipeline.ipynb`** — fetches observed market option "
        "chains (calls + puts at multiple strikes / expiries) and writes "
        "them to parquet. The research notebook joins this output with its "
        "MC simulations to compute MAPEs."
    )

    st.markdown(
        "**`yfinance_pipeline.ipynb`** — pulls daily price history per "
        "ticker and writes parquet snapshots. Used as the upstream source "
        "for both `monte_carlo_simulation` (live MC) and "
        "`mc_vs_actual_test` (research)."
    )

    st.divider()
    st.subheader("Design docs in `databricks/docs/`")
    st.markdown(
        "- **[empirical_simulation_methods.md](databricks/docs/empirical_simulation_methods.md)** "
        "— motivation, math and parameters for `block_bootstrap`, `fhs`, "
        "`fhs_rn`, `analogue` and the EMC correction. Explains *why* "
        "GBM under-prices puts and how each method addresses it.\n"
        "- **[multifractal_method.md](databricks/docs/multifractal_method.md)** "
        "— Mandelbrot's MMAR construction, the lognormal cascade we use, "
        "and the role of the `λ` intermittency parameter."
    )
