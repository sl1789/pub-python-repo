"""Pipeline configuration settings.

Centralizes all configurable parameters: Delta table location,
ticker symbols, and retry/logging settings.
"""

import logging
import os
import re
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger instance."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)
    logger.setLevel(LOG_LEVEL)
    return logger


# ---------------------------------------------------------------------------
# Run ID (unique per notebook execution — used for append-mode Delta writes)
# ---------------------------------------------------------------------------
def make_run_id() -> str:
    """Generate a run_id based on current UTC timestamp.

    Format: YYYY-MM-DDTHHMMSS (ISO-ish, filesystem-safe).
    Call once per notebook execution and pass to all write functions.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%S")


# ---------------------------------------------------------------------------
# Delta Tables (Hive metastore - no Unity Catalog required)
# ---------------------------------------------------------------------------
SCHEMA = "default"

# Input: historical price data
TABLE_NAME = "yfinance_historical_data"
FULL_TABLE_NAME = f"{SCHEMA}.{TABLE_NAME}"

# Output: simulation results (same database as input)
RESULTS_TABLE_NAME = "simulation_results"
FULL_RESULTS_TABLE_NAME = f"{SCHEMA}.{RESULTS_TABLE_NAME}"

# ---------------------------------------------------------------------------
# Tickers
# ---------------------------------------------------------------------------
SP500_TICKER = "^GSPC"

TRADING_COMPANIES: list[str] = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "V", "JNJ",
]

ALL_TICKERS: list[str] = [SP500_TICKER] + TRADING_COMPANIES

# ---------------------------------------------------------------------------
# Retry settings (for Yahoo Finance API calls)
# ---------------------------------------------------------------------------
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

# ---------------------------------------------------------------------------
# Parquet export (Azure ADLS)
# ---------------------------------------------------------------------------
# Driven by environment variables so the storage account name is not
# committed to source. Set these as cluster env vars or job parameters in
# Databricks; locally they can be exported in your shell or .env file.
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
CONTAINER = os.getenv("AZURE_RESULTS_CONTAINER", "results")
EXPORT_PREFIX = os.getenv("AZURE_RESULTS_PREFIX", "export")

# Layout (aligned with databricks/lib/paths.py and the API reader):
#     abfss://<CONTAINER>@<ACCOUNT>.dfs.core.windows.net/<PREFIX>/<dataset>/[ticker=<TICKER>]/data.parquet
#
# The API reads from the directory (picks up all .parquet files inside).
# Each notebook run OVERWRITES the parquet (latest snapshot).
# Historical runs are preserved in Delta tables (append mode with run_id).

_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")
_DATASET_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")


def get_export_path(dataset: str, ticker: str | None = None) -> str:
    """Build the Parquet export directory path for a dataset.

    Returns the DIRECTORY path (no trailing slash) that matches what the API
    reads via pd.read_parquet(). The caller writes a file INSIDE this directory.

    Layout:
        abfss://<CONTAINER>@<ACCOUNT>.dfs.core.windows.net/<PREFIX>/<dataset>/[ticker=<TICKER>]

    Raises:
        RuntimeError: if `AZURE_STORAGE_ACCOUNT` is not configured.
        ValueError: if `dataset` or `ticker` do not match the allowlists.
    """
    if not STORAGE_ACCOUNT:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT env var is not set; cannot build export path"
        )
    if not isinstance(dataset, str) or not _DATASET_RE.match(dataset):
        raise ValueError(f"invalid dataset name: {dataset!r}")
    base = (
        f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"{EXPORT_PREFIX}/{dataset}"
    )
    if ticker is None:
        return base
    if not isinstance(ticker, str) or not _TICKER_RE.match(ticker):
        raise ValueError(f"invalid ticker symbol: {ticker!r}")
    return f"{base}/ticker={ticker}"
