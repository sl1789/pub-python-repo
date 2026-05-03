"""Unit tests for utils.delta_helpers module.

Spark and Delta operations are mocked for fast, isolated unit testing.
For integration tests that require a live Spark session, run these on
a Databricks cluster with pytest.
"""

import datetime
from unittest.mock import patch, MagicMock, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _mock_spark_session():
    """Create a mocked SparkSession."""
    mock_spark = MagicMock()
    return mock_spark


def _mock_row(ticker: str, max_date: datetime.date = None):
    """Create a mocked Row object."""
    row = MagicMock()
    row.ticker = ticker
    if max_date is not None:
        row.max_date = max_date
    return row


# ---------------------------------------------------------------------------
# Tests: get_existing_tickers
# ---------------------------------------------------------------------------

class TestGetExistingTickers:
    """Tests for get_existing_tickers."""

    @patch("utils.delta_helpers._get_spark")
    def test_returns_tickers_when_table_exists(self, mock_get_spark):
        """Returns list of distinct tickers from the table."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_df = MagicMock()
        mock_df.collect.return_value = [
            _mock_row("AAPL"),
            _mock_row("MSFT"),
            _mock_row("^GSPC"),
        ]
        mock_spark.sql.return_value = mock_df

        from utils.delta_helpers import get_existing_tickers

        result = get_existing_tickers("main.default.yfinance_historical_data")

        assert result == ["AAPL", "MSFT", "^GSPC"]
        mock_spark.sql.assert_called_once_with(
            "SELECT DISTINCT ticker FROM main.default.yfinance_historical_data"
        )

    @patch("utils.delta_helpers._get_spark")
    def test_returns_empty_list_when_table_not_found(self, mock_get_spark):
        """Returns empty list if table does not exist."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_spark.sql.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] Table not found: main.default.yfinance_historical_data"
        )

        from utils.delta_helpers import get_existing_tickers

        result = get_existing_tickers("main.default.yfinance_historical_data")

        assert result == []

    @patch("utils.delta_helpers._get_spark")
    def test_raises_on_unexpected_error(self, mock_get_spark):
        """Re-raises unexpected exceptions."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_spark.sql.side_effect = Exception("Permission denied")

        from utils.delta_helpers import get_existing_tickers

        with pytest.raises(Exception, match="Permission denied"):
            get_existing_tickers("main.default.yfinance_historical_data")


# ---------------------------------------------------------------------------
# Tests: get_latest_dates
# ---------------------------------------------------------------------------

class TestGetLatestDates:
    """Tests for get_latest_dates."""

    @patch("utils.delta_helpers._get_spark")
    def test_returns_dict_of_ticker_to_date(self, mock_get_spark):
        """Returns {ticker: max_date} mapping."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_df = MagicMock()
        mock_df.collect.return_value = [
            _mock_row("AAPL", datetime.date(2024, 12, 20)),
            _mock_row("MSFT", datetime.date(2024, 12, 19)),
        ]
        mock_spark.sql.return_value = mock_df

        from utils.delta_helpers import get_latest_dates

        result = get_latest_dates("main.default.yfinance_historical_data")

        assert result == {
            "AAPL": datetime.date(2024, 12, 20),
            "MSFT": datetime.date(2024, 12, 19),
        }

    @patch("utils.delta_helpers._get_spark")
    def test_returns_empty_dict_when_table_not_found(self, mock_get_spark):
        """Returns empty dict if table does not exist."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_spark.sql.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] does not exist"
        )

        from utils.delta_helpers import get_latest_dates

        result = get_latest_dates("main.default.yfinance_historical_data")

        assert result == {}


# ---------------------------------------------------------------------------
# Tests: get_missing_tickers
# ---------------------------------------------------------------------------

class TestGetMissingTickers:
    """Tests for get_missing_tickers."""

    @patch("utils.delta_helpers.get_existing_tickers")
    def test_returns_tickers_not_in_table(self, mock_existing):
        """Returns only tickers missing from the table."""
        mock_existing.return_value = ["AAPL", "MSFT"]

        from utils.delta_helpers import get_missing_tickers

        all_tickers = ["AAPL", "MSFT", "GOOGL", "NVDA"]
        result = get_missing_tickers(all_tickers, "main.default.yfinance_historical_data")

        assert result == ["GOOGL", "NVDA"]

    @patch("utils.delta_helpers.get_existing_tickers")
    def test_returns_all_when_table_empty(self, mock_existing):
        """Returns all tickers when table has no data."""
        mock_existing.return_value = []

        from utils.delta_helpers import get_missing_tickers

        all_tickers = ["^GSPC", "AAPL", "MSFT"]
        result = get_missing_tickers(all_tickers, "main.default.yfinance_historical_data")

        assert result == ["^GSPC", "AAPL", "MSFT"]

    @patch("utils.delta_helpers.get_existing_tickers")
    def test_returns_empty_when_all_present(self, mock_existing):
        """Returns empty list when all tickers are already loaded."""
        mock_existing.return_value = ["AAPL", "MSFT", "GOOGL"]

        from utils.delta_helpers import get_missing_tickers

        result = get_missing_tickers(
            ["AAPL", "MSFT", "GOOGL"],
            "main.default.yfinance_historical_data",
        )

        assert result == []


# ---------------------------------------------------------------------------
# Tests: merge_to_delta
# ---------------------------------------------------------------------------

class TestMergeToDelta:
    """Tests for merge_to_delta."""

    @patch("utils.delta_helpers.DeltaTable")
    @patch("utils.delta_helpers._get_spark")
    def test_creates_table_when_not_exists(self, mock_get_spark, mock_delta_table):
        """Creates a new table on first run."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark
        mock_spark.catalog.tableExists.return_value = False

        mock_df = MagicMock()
        mock_df.count.return_value = 100

        # Mock the final stats queries
        mock_count_row = MagicMock()
        mock_count_row.total_rows = 100
        mock_ticker_row = MagicMock()
        mock_ticker_row.num_tickers = 5
        mock_spark.sql.return_value.collect.side_effect = [
            [mock_count_row],
            [mock_ticker_row],
        ]

        from utils.delta_helpers import merge_to_delta

        merge_to_delta(mock_df, "main.default.yfinance_historical_data")

        mock_df.write.format.assert_called_once_with("delta")
        mock_df.write.format().mode.assert_called_once_with("overwrite")
        mock_df.write.format().mode().saveAsTable.assert_called_once_with(
            "main.default.yfinance_historical_data"
        )

    @patch("utils.delta_helpers.DeltaTable")
    @patch("utils.delta_helpers._get_spark")
    def test_merges_when_table_exists(self, mock_get_spark, mock_delta_table):
        """Performs MERGE (upsert) on existing table."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark
        mock_spark.catalog.tableExists.return_value = True

        mock_df = MagicMock()
        mock_df.count.return_value = 50

        # Mock DeltaTable.forName
        mock_dt_instance = MagicMock()
        mock_delta_table.forName.return_value = mock_dt_instance

        # Mock the final stats queries
        mock_count_row = MagicMock()
        mock_count_row.total_rows = 500
        mock_ticker_row = MagicMock()
        mock_ticker_row.num_tickers = 11
        mock_spark.sql.return_value.collect.side_effect = [
            [mock_count_row],
            [mock_ticker_row],
        ]

        from utils.delta_helpers import merge_to_delta

        merge_to_delta(mock_df, "main.default.yfinance_historical_data")

        # Verify merge was called
        mock_dt_instance.alias.assert_called_once_with("target")
        mock_dt_instance.alias().merge.assert_called_once()

    @patch("utils.delta_helpers._get_spark")
    def test_skips_when_empty_dataframe(self, mock_get_spark):
        """Does nothing when DataFrame has 0 rows."""
        mock_spark = _mock_spark_session()
        mock_get_spark.return_value = mock_spark

        mock_df = MagicMock()
        mock_df.count.return_value = 0

        from utils.delta_helpers import merge_to_delta

        merge_to_delta(mock_df, "main.default.yfinance_historical_data")

        # Should not attempt to write or merge
        mock_spark.catalog.tableExists.assert_not_called()
