"""Pipeline configuration settings.

Centralizes all configurable parameters: Delta table location,
ticker symbols, and retry/logging settings.
"""

import logging
import os
import re

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
# ---------------------------------------------------------------------------
# Parquet export (Azure ADLS)
# ---------------------------------------------------------------------------
# Driven by environment variables so the storage account name is not
# committed to source. Set these as cluster env vars or job parameters in
# Databricks; locally they can be exported in your shell or .env file.
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
CONTAINER = os.getenv("AZURE_RESULTS_CONTAINER", "results")
EXPORT_PREFIX = os.getenv("AZURE_RESULTS_PREFIX", "export/simulations")

# Same allowlist as in utils.simulation_helpers; mirrored here so this module
# stays import-cycle free.
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")


def get_export_path(ticker: str) -> str:
    """Build the Parquet export path for a given ticker.

    Raises:
        RuntimeError: if `AZURE_STORAGE_ACCOUNT` is not configured.
        ValueError: if `ticker` does not match the allowlist (prevents path
            traversal and abfss:// URI injection).
    """
    if not STORAGE_ACCOUNT:
        raise RuntimeError(
            "AZURE_STORAGE_ACCOUNT env var is not set; cannot build export path"
        )
    if not isinstance(ticker, str) or not _TICKER_RE.match(ticker):
        raise ValueError(f"invalid ticker symbol: {ticker!r}")
    return (
        f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
        f"{EXPORT_PREFIX}/ticker={ticker}/"
    )
