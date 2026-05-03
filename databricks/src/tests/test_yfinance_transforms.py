"""Unit tests for transforms.yfinance_transforms module.

All Yahoo Finance API calls are mocked to ensure tests are deterministic,
fast, and do not depend on network access.
"""

import datetime
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_sample_ohlcv(n_days: int = 5, start_price: float = 100.0) -> pd.DataFrame:
    """Create a realistic OHLCV DataFrame as returned by yf.download."""
    dates = pd.date_range(start="2024-01-02", periods=n_days, freq="B")
    np.random.seed(42)
    close_prices = start_price + np.cumsum(np.random.randn(n_days))
    data = pd.DataFrame(
        {
            "Open": close_prices - 0.5,
            "High": close_prices + 1.0,
            "Low": close_prices - 1.0,
            "Close": close_prices,
            "Adj Close": close_prices,
            "Volume": np.random.randint(1_000_000, 10_000_000, size=n_days),
        },
        index=dates,
    )
    data.index.name = "Date"
    return data


# ---------------------------------------------------------------------------
# Tests: download_and_transform
# ---------------------------------------------------------------------------

class TestDownloadAndTransform:
    """Tests for the download_and_transform function."""

    @patch("transforms.yfinance_transforms.yf.download")
    def test_successful_full_download(self, mock_download):
        """Full history download returns expected columns and no NaN rows."""
        mock_download.return_value = _make_sample_ohlcv(n_days=5)

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("AAPL", start_date=None)

        # Should have n_days - 1 rows (first row dropped due to NaN)
        assert len(result) == 4
        assert list(result.columns) == [
            "ticker", "Date", "Open", "High", "Low",
            "Close", "Adj_Close", "Volume", "pct_change", "log_return",
        ]
        assert (result["ticker"] == "AAPL").all()
        assert result["pct_change"].isna().sum() == 0
        assert result["log_return"].isna().sum() == 0

    @patch("transforms.yfinance_transforms.yf.download")
    def test_pct_change_calculation(self, mock_download):
        """pct_change is calculated correctly as (P1 - P0) / P0."""
        mock_download.return_value = _make_sample_ohlcv(n_days=3)

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("MSFT", start_date=None)

        # Manually compute expected pct_change for row index 1 (second trading day)
        raw = _make_sample_ohlcv(n_days=3)
        adj_close = raw["Adj Close"]
        expected_pct = (adj_close.iloc[1] - adj_close.iloc[0]) / adj_close.iloc[0]

        assert pytest.approx(result.iloc[0]["pct_change"], rel=1e-6) == expected_pct

    @patch("transforms.yfinance_transforms.yf.download")
    def test_log_return_calculation(self, mock_download):
        """log_return is calculated as ln(P1 / P0)."""
        mock_download.return_value = _make_sample_ohlcv(n_days=3)

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("GOOGL", start_date=None)

        raw = _make_sample_ohlcv(n_days=3)
        adj_close = raw["Adj Close"]
        expected_log = np.log(adj_close.iloc[1] / adj_close.iloc[0])

        assert pytest.approx(result.iloc[0]["log_return"], rel=1e-6) == expected_log

    @patch("transforms.yfinance_transforms.yf.download")
    def test_empty_response_returns_empty_df(self, mock_download):
        """When yfinance returns no data, an empty DataFrame is returned."""
        mock_download.return_value = pd.DataFrame()

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("INVALID", start_date=None)

        assert result.empty

    @patch("transforms.yfinance_transforms.yf.download")
    def test_incremental_download_uses_start_date(self, mock_download):
        """Incremental mode passes start_date + 1 day to yf.download."""
        mock_download.return_value = _make_sample_ohlcv(n_days=3)

        from transforms.yfinance_transforms import download_and_transform

        start = datetime.date(2024, 6, 15)
        download_and_transform("TSLA", start_date=start)

        # Verify yf.download was called with start='2024-06-16'
        mock_download.assert_called_once_with(
            "TSLA", start="2024-06-16", progress=False
        )

    @patch("transforms.yfinance_transforms.yf.download")
    def test_full_download_uses_period_max(self, mock_download):
        """Full history mode passes period='max' to yf.download."""
        mock_download.return_value = _make_sample_ohlcv(n_days=3)

        from transforms.yfinance_transforms import download_and_transform

        download_and_transform("META", start_date=None)

        mock_download.assert_called_once_with(
            "META", period="max", progress=False
        )

    @patch("transforms.yfinance_transforms.yf.download")
    def test_multiindex_columns_are_flattened(self, mock_download):
        """MultiIndex columns (as sometimes returned by yfinance) are handled."""
        raw = _make_sample_ohlcv(n_days=4)
        # Simulate MultiIndex columns: (Price, Ticker)
        raw.columns = pd.MultiIndex.from_tuples(
            [(col, "NVDA") for col in raw.columns]
        )
        mock_download.return_value = raw

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("NVDA", start_date=None)

        # Should still produce valid output
        assert "Adj_Close" in result.columns
        assert len(result) == 3  # 4 days - 1 NaN row

    @patch("transforms.yfinance_transforms.time.sleep")
    @patch("transforms.yfinance_transforms.yf.download")
    def test_retry_on_exception(self, mock_download, mock_sleep):
        """Retries with backoff when yf.download raises an exception."""
        # Fail twice, succeed on third attempt
        mock_download.side_effect = [
            Exception("Connection timeout"),
            Exception("Connection timeout"),
            _make_sample_ohlcv(n_days=3),
        ]

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("JPM", start_date=None)

        assert not result.empty
        assert mock_download.call_count == 3
        # Verify backoff sleeps were called
        assert mock_sleep.call_count == 2

    @patch("transforms.yfinance_transforms.time.sleep")
    @patch("transforms.yfinance_transforms.yf.download")
    def test_all_retries_exhausted_returns_empty(self, mock_download, mock_sleep):
        """After MAX_RETRIES failures, returns empty DataFrame."""
        mock_download.side_effect = Exception("Persistent failure")

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("FAIL", start_date=None)

        assert result.empty

    @patch("transforms.yfinance_transforms.yf.download")
    def test_date_column_is_date_type(self, mock_download):
        """The Date column should contain datetime.date objects, not timestamps."""
        mock_download.return_value = _make_sample_ohlcv(n_days=5)

        from transforms.yfinance_transforms import download_and_transform

        result = download_and_transform("V", start_date=None)

        assert all(isinstance(d, datetime.date) for d in result["Date"])
