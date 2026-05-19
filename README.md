# pub-python-repo

A small **job orchestration platform** built with FastAPI, SQLModel, and Streamlit.
It exposes a REST API for submitting Monte Carlo option pricing jobs to a
Databricks notebook, tracking their lifecycle, and retrieving the resulting
rows from an Azure ADLS Gen2 parquet export.

## Architecture overview

```
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
```

## Functionality implemented so far

### REST API ([app/main.py](app/main.py))
FastAPI application wiring routers, middleware, exception handlers and DB
bootstrap on startup.

- **Health** ([app/api/health.py](app/api/health.py)) – `GET /health` liveness probe.
- **Auth** ([app/api/auth.py](app/api/auth.py)) – `POST /auth/token` OAuth2
  password flow returning a JWT bearer token.
- **Jobs** ([app/api/jobs.py](app/api/jobs.py))
  - `POST /jobs` – submit a Monte Carlo job (requires `submitter` role); the
    request body is `{ticker, strike, period_days, num_simulations}` and
    `runner` is forced to `"databricks"`. The job is stored as `QUEUED`.
  - `GET /jobs/{job_id}` – fetch a single job (any authenticated user).
  - `GET /jobs` – paginated listing with optional status filter (requires
    `viewer` role).
- **Results** ([app/api/results.py](app/api/results.py))
  - `GET /results?job_id=` – returns simulation rows for a job; enforces that
    the job is `SUCCEEDED` (HTTP 409 otherwise) and reads the parquet export
    referenced by the job's `output_ref`.

### Authentication & authorization ([app/core/security.py](app/core/security.py))
- Password hashing with `passlib`/bcrypt.
- JWT issuance and verification with a server-forced algorithm.
- `get_current_user` dependency and `require_roles(...)` RBAC dependency.
- Demo user seeded from environment variables
  (`DEMO_USER_USERNAME`, `DEMO_USER_PASSWORD`, `DEMO_USER_ROLES`).

### Middleware
- **Request correlation** ([app/core/middleware.py](app/core/middleware.py)) –
  injects/propagates an `X-Request-ID` per request.
- **Security middleware** ([app/core/security_middleware.py](app/core/security_middleware.py))
  - `SecurityHeadersMiddleware` – adds standard hardening headers.
  - `EnforceJsonContentTypeMiddleware` – rejects non-JSON bodies.
  - `RateLimitMiddleware` – simple in-memory rate limiter (120 req / 60 s).
- **Centralised error handling** ([app/core/errors.py](app/core/errors.py)) –
  uniform JSON error envelopes for HTTP and validation errors.
- **Structured logging** ([app/core/logging_config.py](app/core/logging_config.py))
  – JSON logger setup helper.

### Persistence ([app/db](app/db))
SQLModel/SQLAlchemy models on SQLite by default (`DATABASE_URL` configurable):
- `Job` – status, params (JSON), runner, external run id, timestamps,
  `output_ref`, error message.
- `ResultRow` – per-job business-date metric rows.
- `JobEvent` – audit/event trail per job.
- `JobMetrics` – run duration, row counts, runner, status.

Schema is managed with **Alembic** ([migrations/](migrations/), [alembic.ini](alembic.ini)),
plus an `init_db_and_seed` helper for development bootstrap
([app/db/init_db.py](app/db/init_db.py)).

### Background worker ([worker/worker.py](worker/worker.py))
A polling loop that:
1. Polls all `RUNNING` jobs through the runner and applies state updates.
2. Picks up the next `QUEUED` job (FIFO), submits it via `DatabricksRunner`,
   and transitions it to `RUNNING` with an `external_run_id` and `output_ref`.
3. Marks failures with an error message.

### Runner ([app/runners](app/runners))
[DatabricksRunner](app/runners/databricks.py) – submits and polls Databricks
Jobs via the REST API. Constructs the parquet `output_ref` at submit time
using the shared [databricks/lib/paths.py](databricks/lib/paths.py) helpers.

### Results repository ([app/results](app/results))
[AzureParquetResultsRepository](app/results/parquet_azure.py) reads the
ticker-partitioned parquet export written by the
`monte_carlo_simulation` notebook via `adlfs`/`pyarrow` and filters rows down
to the (ticker, K, T, Runs) combination of the requesting job.

### Schemas ([app/schemas](app/schemas))
Pydantic request/response models for jobs, results, and error envelopes.

### Streamlit UI ([ui/streamlit_app.py](ui/streamlit_app.py))
Small operator UI that:
- Logs in against `/auth/token` and stores the JWT in session state.
- Submits Monte Carlo jobs (ticker, strike K, period T, number of simulations).
- Polls job status and renders result rows in a dataframe once the job is
  `SUCCEEDED`.

The HTTP client lives in [ui/api_client.py](ui/api_client.py).

### Databricks job assets ([databricks/](databricks/))

Monte Carlo simulation notebook executed by the Databricks runner
(`databricks/jobs/monte_carlo_simulation.ipynb`).

#### Monte Carlo Options Pricing Framework

A NumPy-vectorized Monte Carlo simulation engine for pricing European options,
benchmarked against the Black-Scholes analytical formula and validated against
live market prices from Yahoo Finance.

##### Source modules ([databricks/src/](databricks/src/))

| Module | Purpose |
| --- | --- |
| `config/settings.py` | Table names, export paths, structured logging |
| `transforms/simulation.py` | 6 pure NumPy simulation functions + `SIMULATION_METHODS` registry |
| `utils/simulation_helpers.py` | Data loading, distribution fitting, risk-free rate (^IRX), run orchestration |
| `utils/delta_helpers.py` | Delta table read/write utilities |
| `tests/` | Unit tests for transforms and Delta helpers |

##### Notebooks ([databricks/jobs/](databricks/jobs/))

| Notebook | Purpose |
| --- | --- |
| `monte_carlo_simulation` | Main simulation pipeline (single ticker, configurable runs/T/K) |
| `scalability_test` | Convergence testing across scales (1K–10M) and alt_weights (0.1–0.5) |
| `options_data_pipeline` | Fetches live option chains from Yahoo Finance for 6 tickers |
| `mc_vs_actual_test` | Compares MC estimates to actual market prices (accuracy analysis) |

##### Delta tables

| Table | Rows | Description |
| --- | --- | --- |
| `default.yfinance_historical_data` | 105K | Daily OHLCV + log returns for 11 tickers (^GSPC since 1927) |
| `default.simulation_results` | varies | MC simulation outputs (partitioned by ticker, append mode) |
| `default.actual_option_prices` | ~700 | Live option quotes: SPY, AAPL, MSFT, GOOGL, AMZN, META |

##### Simulation methods

1. **historical** – Random sampling from historical log returns (with replacement)
2. **window** – Consecutive T-day window from random start (wrapping at array boundary)
3. **window_10d** – Window with 10-day step size
4. **window_20d** – Window with 20-day step size
5. **student_t** – `(1 − alt_weight)` × historical + `alt_weight` × Student-t draws
6. **black_scholes** – GBM under risk-neutral measure; uses live risk-free rate from ^IRX and annualized historical volatility; discounted to present value
7. **multifractal** – Mandelbrot MMAR "baby" model: risk-neutral GBM subordinated to a lognormal multiplicative cascade of trading time, producing fat tails and volatility clustering; intermittency parameter `lam` (default 0.4) controls cascade variance; discounted to present value. See [databricks/docs/multifractal_method.md](databricks/docs/multifractal_method.md) for derivation and references.
8. **multifractal_empirical** – Same MMAR cascade as `multifractal`, but the per-step Gaussian shocks are replaced with standardised residuals resampled from the historical log-return series. Combines cascade-driven clustering with empirically fat / skewed shocks; risk-neutral and discounted.
9. **block_bootstrap** – Politis–Romano stationary bootstrap: concatenates consecutive blocks (geometric lengths, mean `block_mean_len` ≈ 5 days) drawn from history. Preserves short-range autocorrelation and volatility clustering, fit-free.
10. **fhs** – Filtered Historical Simulation under the physical measure: GARCH(1,1) MLE fit + resampling of standardised residuals, rolled forward through the conditional-variance recursion starting from today's vol regime. Empirical residuals provide fat tails; GARCH dynamics provide clustering.
11. **fhs_rn** – FHS under the risk-neutral measure: same simulator with daily drift replaced by `r/252 − ½σ²`; combined with the Empirical Martingale Correction (EMC) and PV discount, yields a no-arbitrage-consistent option price.
12. **analogue** – State-conditional k-nearest-neighbour bootstrap: at each step, each path queries a KD-tree of historical days for the `k` whose recent 5-day return/vol features most resemble the path's own current state, then samples one of their actual next-day returns. Markov-conditional rather than unconditional.

See [databricks/docs/empirical_simulation_methods.md](databricks/docs/empirical_simulation_methods.md) for the theory behind the bootstrap / FHS / EMC / analogue family.

##### Running the framework

```
# 1. Ensure historical data exists
#    (default.yfinance_historical_data must be populated)

# 2. Run the options data pipeline (fetches live quotes)
Run: databricks/jobs/options_data_pipeline

# 3. Run the main simulation
Run: databricks/jobs/monte_carlo_simulation

# 4. Run scalability / convergence tests
Run: databricks/jobs/scalability_test

# 5. Compare MC prices to actual market prices
Run: databricks/jobs/mc_vs_actual_test
```

##### Key results

| Metric | Best method | MAPE | Notes |
| --- | --- | --- | --- |
| All strikes – Calls | black_scholes | 46.4% | Risk-neutral pricing aligns with market convention |
| All strikes – Puts | window | 50.0% | Empirical distribution captures real-world put dynamics |
| ATM only – Calls | window | 53.2% | Near-tied with BS (53.6%) |
| ATM only – Puts | window | 28.5% | Dramatic improvement; only +1.8% median bias |

- All methods systematically overprice vs market (historical vol > implied vol)
- Error stabilises by 500K simulation runs; no improvement beyond that
- Convergence: all methods achieve < 0.05% change at 5M → 10M scale transition
- Performance: ~25s for 10M runs on a single Standard_D8ds_v4 node (32 GB RAM)

### Infrastructure ([infra/nginx/nginx.conf](infra/nginx/nginx.conf))
Sample reverse-proxy configuration for fronting the FastAPI service.

### Tests ([tests/](tests/))
Pytest suite covering the API end to end:
- [test_health.py](tests/test_health.py)
- [test_jobs.py](tests/test_jobs.py)
- [test_results_flow.py](tests/test_results_flow.py)

Shared fixtures in [tests/conftest.py](tests/conftest.py).

## Running locally

Common targets are wired up in the [Makefile](Makefile):

```bash
make venv       # create virtual environment
make install    # install dependencies
make api        # run FastAPI on 127.0.0.1:8000
make ui         # run Streamlit UI
make worker     # run the background worker loop
make test       # run pytest
```

## Configuration

Environment variables are loaded via `python-dotenv`
([app/core/config.py](app/core/config.py)). Key settings:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | SQLAlchemy URL (default `sqlite:///./app.db`) |
| `JWT_SECRET`, `JWT_ALGORITHM`, `JWT_EXPIRES_MINUTES` | Token signing |
| `DEMO_USER_USERNAME` / `_PASSWORD` / `_ROLES` | Seeded demo user |
| `API_HOST`, `API_PORT` | API bind address |
| `AZURE_STORAGE_ACCOUNT`, `AZURE_RESULTS_CONTAINER`, `AZURE_MC_RESULTS_PREFIX`, `AZURE_STORAGE_KEY` | Azure parquet export read by the API (must match the notebook's writer) |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_MC_JOB_ID` | Databricks runner |

## Tech stack

FastAPI, Starlette, SQLModel/SQLAlchemy, Alembic, Pydantic v2,
Passlib (bcrypt), python-jose, PyArrow, adlfs/fsspec, PySpark, Streamlit,
Uvicorn, pytest, NumPy, SciPy, yfinance, matplotlib.

## Changelog

### 2026-05-06 — Market Validation & ATM Analysis

- Added ATM-only accuracy analysis (|K/S0 - 1| ≤ 5%) to `mc_vs_actual_test`
- ATM puts: MAPE improved from 50.0% to 28.5% (window method, +1.8% median bias)
- ATM calls: window method (53.2%) narrowly beats Black-Scholes (53.6%)
- Updated README with full Monte Carlo framework documentation

### 2026-05-06 — Options Data Pipeline & MC vs Actual Test

- Created `options_data_pipeline` notebook: fetches live option chains from
  Yahoo Finance for SPY, AAPL, MSFT, GOOGL, AMZN, META (~120 rows each)
- Created `default.actual_option_prices` Delta table (716 rows, 6 tickers,
  5–52 days to expiry, calls + puts)
- Created `mc_vs_actual_test` notebook: compares MC simulation prices against
  actual market mid-prices across 5 scales (10K–2M runs)
- Fixed SPY/^GSPC scale mismatch: use `underlying_price` from options data
  as S0, not the ^GSPC index level (10x difference)
- Key finding: Black-Scholes best for calls (46.4% MAPE), window best for
  puts (50.0% MAPE); all methods overprice vs market

### 2026-05-06 — Black-Scholes Benchmark

- Added `sim_black_scholes()` to `simulation.py`: GBM under risk-neutral
  measure with daily time steps (dt = 1/252)
- Added `get_risk_free_rate()` to `simulation_helpers.py`: fetches ^IRX
  (13-week T-bill) from yfinance with 4.5% fallback
- Annualised volatility calculation: daily_std × √252
- Discounting: applies exp(-r × T/252) only for black_scholes method
- Added analytical BS comparison and put-call parity check to
  `monte_carlo_simulation` notebook
- Updated `scalability_test` with BS benchmark bar chart and price comparison

### 2026-05-05 — Scalability & Convergence Testing

- Created `scalability_test` notebook: tests all methods at scales 1K–10M
  with 5 alt_weight levels (0.1–0.5)
- Convergence criterion: < 1% price change at highest scale transition
- All deterministic methods converge perfectly; student_t fails at
  alt_weight=0.5 (heavy tails, df ≈ 2.5)
- Performance: 10M runs in ~25s on single Standard_D8ds_v4 node

### 2026-05-04 — Simulation Engine & Methods

- Built NumPy-vectorized simulation engine (no Spark UDFs)
- Implemented 5 empirical methods: historical, window, window_10d,
  window_20d, student_t
- Added `_MAX_LOG_SUM = 10.0` clipping to prevent overflow
- Removed cauchy (infinite variance) and exponential (outlier) methods
- Created `monte_carlo_simulation` notebook as main pipeline

### 2026-05-03 — Historical Data Pipeline

- Created `yfinance_pipeline` for fetching OHLCV data from Yahoo Finance
- Populated `default.yfinance_historical_data` (105K rows, 11 tickers)
- ^GSPC history from 1927 (24,699 rows); individual stocks from IPO date
- Log returns computed as ln(Close_t / Close_{t-1})
