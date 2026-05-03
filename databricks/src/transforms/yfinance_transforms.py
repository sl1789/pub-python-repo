"""Yahoo Finance data download and transformation logic.

Downloads OHLCV data from Yahoo Finance and computes derived columns:
- pct_change: daily percentage change of Adjusted Close
- log_return: log return of Adjusted Close

Includes retry logic to handle transient API failures.

Note: yfinance >= 0.2.31 removed the 'Adj Close' column.
      'Close' now represents the adjusted close by default.
"""

from __future__ import annotations

import datetime
import time

import numpy as np
import pandas as pd
import yfinance as yf

from config.settings import MAX_RETRIES, RETRY_BACKOFF_SECONDS, get_logger

logger = get_logger(__name__)


def _download_with_retry(
    ticker: str,
    start: str | None = None,
    period: str | None = None,
) -> pd.DataFrame:
    """Download data from Yahoo Finance with exponential backoff retry.

    Args:
        ticker: Stock ticker symbol.
        start: Start date string (YYYY-MM-DD) for incremental downloads.
        period: Period string (e.g. 'max') for full history downloads.

    Returns:
        Raw pandas DataFrame from yfinance, or empty DataFrame on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if start:
                data = yf.download(ticker, start=start, progress=False)
            else:
                data = yf.download(ticker, period=period or "max", progress=False)

            if data.empty and attempt < MAX_RETRIES:
                logger.warning(
                    f"Empty response for {ticker} (attempt {attempt}/{MAX_RETRIES}). Retrying..."
                )
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue

            return data

        except Exception as e:
            logger.error(f"Error downloading {ticker} (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            else:
                logger.error(f"All {MAX_RETRIES} attempts failed for {ticker}.")
                return pd.DataFrame()

    return pd.DataFrame()


def download_and_transform(
    ticker: str,
    start_date: datetime.date | None = None,
) -> pd.DataFrame:
    """Download historical data for a ticker and apply transformations.

    Args:
        ticker: Stock ticker symbol.
        start_date: If provided, only data AFTER this date is downloaded
                    (incremental mode). If None, full history is downloaded.

    Returns:
        Transformed pandas DataFrame with columns:
        [ticker, Date, Open, High, Low, Close, Adj_Close, Volume, pct_change, log_return]
    """
    if start_date is not None:
        fetch_start = start_date + datetime.timedelta(days=1)
        logger.info(f"Downloading INCREMENTAL data for {ticker} (from {fetch_start})")
        data = _download_with_retry(ticker, start=str(fetch_start))
    else:
        logger.info(f"Downloading FULL history for {ticker}")
        data = _download_with_retry(ticker, period="max")

    if data.empty:
        logger.warning(f"No new data returned for {ticker}")
        return pd.DataFrame()

    # Flatten MultiIndex columns if present
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # Handle Adj Close column (removed in yfinance >= 0.2.31)
    # In newer versions, 'Close' already represents the adjusted close.
    if "Adj Close" in data.columns:
        data = data.rename(columns={"Adj Close": "Adj_Close"})
    else:
        data["Adj_Close"] = data["Close"]

    # Compute derived columns
    data["pct_change"] = data["Adj_Close"].pct_change(1)
    data["log_return"] = np.log(data["Adj_Close"] / data["Adj_Close"].shift(1))

    # Add ticker identifier
    data["ticker"] = ticker

    # Reset index to get Date as a column
    data = data.reset_index()
    data["Date"] = pd.to_datetime(data["Date"]).dt.date

    # Select and reorder columns
    columns_to_keep = [
        "ticker", "Date", "Open", "High", "Low",
        "Close", "Adj_Close", "Volume", "pct_change", "log_return",
    ]
    data = data[columns_to_keep]

    # Drop rows with NaN (first row has NaN for pct_change/log_return)
    data = data.dropna()

    logger.info(f"Downloaded {len(data)} rows for {ticker}")
    return data
