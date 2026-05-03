"""Pipeline configuration settings.

Centralizes all configurable parameters: Delta table location,
ticker symbols, and retry/logging settings.
"""

import logging

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
# Parquet export (Azure ADLS)
# ---------------------------------------------------------------------------
STORAGE_ACCOUNT = "esgteamstorage"
CONTAINER = "results"
EXPORT_PREFIX = "export/simulations"


def get_export_path(ticker: str) -> str:
    """Build the Parquet export path for a given ticker."""
    return f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{EXPORT_PREFIX}/ticker={ticker}/"
