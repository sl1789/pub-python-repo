# pub-python-repo

A small **job orchestration platform** built with FastAPI, SQLModel, and Streamlit.
It exposes a REST API for submitting, tracking, and retrieving the results of
data jobs that can be executed by pluggable runners (local, Databricks, Airflow)
and persisted to pluggable result backends (local Parquet, Azure ADLS Parquet,
or the database).

## Architecture overview

```
┌──────────────┐     ┌──────────────┐     ┌────────────────────┐
│ Streamlit UI │ ──► │  FastAPI API │ ──► │  SQLite / SQLModel │
└──────────────┘     └──────┬───────┘     └────────────────────┘
                            │
                            ▼
                     ┌──────────────┐     ┌────────────────────┐
                     │  Worker loop │ ──► │ Runners (local /   │
                     │  (poller)    │     │ databricks/airflow)│
                     └──────┬───────┘     └────────────────────┘
                            │
                            ▼
                     ┌──────────────────────────────────────┐
                     │ Results repos (parquet local/Azure)  │
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
  - `POST /jobs` – submit a job (requires `submitter` role); validates date
    range and stores params, runner, and status `QUEUED`.
  - `GET /jobs/{job_id}` – fetch a single job (any authenticated user).
  - `GET /jobs` – paginated listing with optional status filter (requires
    `viewer` role).
- **Results** ([app/api/results.py](app/api/results.py))
  - `GET /results?job_id=&start_date=&end_date=` – returns rows for a job;
    enforces that the job is `SUCCEEDED` (HTTP 409 otherwise) and dispatches to
    the appropriate results repository based on the job's `output_ref`.

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
1. Polls all `RUNNING` jobs through their runner and applies state updates.
2. Picks up the next `QUEUED` job (FIFO):
   - For `LocalRunner`, executes synchronously and persists results.
   - For external runners, submits the job and transitions it to `RUNNING`
     with an `external_run_id`.
3. Marks failures with an error message.

### Pluggable runners ([app/runners](app/runners))
Common `BaseRunner` interface ([app/runners/base.py](app/runners/base.py)) with a
`get_runner(name)` factory ([app/runners/factory.py](app/runners/factory.py)):
- [LocalRunner](app/runners/local.py) – in-process toy execution producing
  metric rows.
- [DatabricksRunner](app/runners/databricks.py) – submits and polls Databricks
  jobs via the REST API.
- [AirflowRunner](app/runners/airflow.py) – triggers and polls Airflow DAG runs.

### Pluggable result repositories ([app/results](app/results))
Common `ResultsRepository` interface ([app/results/base.py](app/results/base.py)),
selected by the `output_ref` scheme via [app/results/factory.py](app/results/factory.py):
- [parquet_local.py](app/results/parquet_local.py) – local filesystem Parquet.
- [parquet_azure.py](app/results/parquet_azure.py) – Azure ADLS Gen2 Parquet
  via `adlfs` / `pyarrow`.

### Schemas ([app/schemas](app/schemas))
Pydantic request/response models for jobs, results, and error envelopes.

### Streamlit UI ([ui/streamlit_app.py](ui/streamlit_app.py))
Small operator UI that:
- Logs in against `/auth/token` and stores the JWT in session state.
- Submits jobs (date range, runner, optional country filter).
- Polls job status and renders result rows in a dataframe once the job is
  `SUCCEEDED`.

The HTTP client lives in [ui/api_client.py](ui/api_client.py).

### Databricks job assets ([databricks/jobs](databricks/jobs))
Reference notebook and Python entry point used by the Databricks runner
(`simple_job_results.ipynb`, `simple_job_results.py`).

### Infrastructure ([infra/nginx/nginx.conf](infra/nginx/nginx.conf))
Sample reverse-proxy configuration for fronting the FastAPI service.

### Tests ([tests/](tests/))
Pytest suite covering the API end to end:
- [test_health.py](tests/test_health.py)
- [test_jobs.py](tests/test_jobs.py)
- [test_job_lifecycle.py](tests/test_job_lifecycle.py)
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
| `AZURE_STORAGE_ACCOUNT`, `AZURE_RESULTS_CONTAINER`, `AZURE_RESULTS_PREFIX`, `AZURE_STORAGE_KEY` | Azure Parquet results backend |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_JOB_ID` | Databricks runner |

## Tech stack

FastAPI, Starlette, SQLModel/SQLAlchemy, Alembic, Pydantic v2,
Passlib (bcrypt), python-jose, PyArrow, adlfs/fsspec, PySpark, Streamlit,
Uvicorn, pytest.
