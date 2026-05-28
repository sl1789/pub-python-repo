# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# Single image used by all three services (api, worker, ui).
# Multi-stage with uv for fast, reproducible installs.
# ---------------------------------------------------------------------------

# ---- builder ----
FROM python:3.13-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# uv: standalone, no system Python deps required.
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /usr/local/bin/

# Build deps only needed if a wheel is unavailable for the target arch.
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (cached) — only requires lock + manifest.
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-project --no-dev || uv sync --no-install-project --no-dev

# Now copy the source and install the project itself.
COPY app ./app
COPY worker ./worker
COPY ui ./ui
COPY databricks ./databricks
COPY migrations ./migrations
COPY alembic.ini ./
RUN uv sync --no-dev || uv sync --no-dev


# ---- runtime ----
FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    APP_ENV=prod

# Non-root user; /data is a mount point for the SQLite file.
RUN groupadd --system --gid 1001 app \
 && useradd --system --uid 1001 --gid 1001 --home /app app \
 && mkdir -p /data \
 && chown -R app:app /data

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app /app
RUN chown -R app:app /app

USER app

# Defaults — all three services share the same image; compose overrides
# the command/port per service.
EXPOSE 8000 8501

# Generic healthcheck is per-service in docker-compose.yml.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
