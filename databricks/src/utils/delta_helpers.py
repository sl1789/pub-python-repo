"""Delta table helper functions.

Provides utilities for checking existing tickers, retrieving latest dates,
identifying missing tickers, and merging data into a Delta table.
"""

from __future__ import annotations

import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from config.settings import FULL_TABLE_NAME, get_logger

logger = get_logger(__name__)


def _get_spark() -> SparkSession:
    """Retrieve the active SparkSession."""
    return SparkSession.getActiveSession()


def get_existing_tickers(table_name: str = FULL_TABLE_NAME) -> list[str]:
    """Return distinct tickers already present in the Delta table.

    Returns an empty list if the table does not exist yet.
    """
    spark = _get_spark()
    try:
        existing_df = spark.sql(f"SELECT DISTINCT ticker FROM {table_name}")
        existing_tickers = [row.ticker for row in existing_df.collect()]
        logger.info(f"Existing tickers in table: {existing_tickers}")
        return existing_tickers
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            logger.info(f"Table {table_name} does not exist yet. Will create it.")
            return []
        raise


def get_latest_dates(table_name: str = FULL_TABLE_NAME) -> dict[str, datetime.date]:
    """Get the most recent date for each ticker in the Delta table.

    Returns:
        A dict mapping ticker -> latest date.
        Empty dict if the table does not exist.
    """
    spark = _get_spark()
    try:
        latest_df = spark.sql(
            f"SELECT ticker, MAX(Date) AS max_date FROM {table_name} GROUP BY ticker"
        )
        latest_dates = {row.ticker: row.max_date for row in latest_df.collect()}
        logger.info(f"Latest dates per ticker: {latest_dates}")
        return latest_dates
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return {}
        raise


def get_missing_tickers(
    all_tickers: list[str], table_name: str = FULL_TABLE_NAME
) -> list[str]:
    """Return tickers from *all_tickers* that are NOT yet in the Delta table."""
    existing = get_existing_tickers(table_name)
    missing = [t for t in all_tickers if t not in existing]
    logger.info(f"New tickers to download (full history): {missing}")
    return missing


def merge_to_delta(
    df: DataFrame, table_name: str = FULL_TABLE_NAME
) -> None:
    """Upsert a Spark DataFrame into the Delta table.

    - If the table does not exist, it is created.
    - If it exists, a MERGE (upsert) on (ticker, Date) is performed.
    """
    spark = _get_spark()
    row_count = df.count()

    if row_count == 0:
        logger.info("No rows to merge. Skipping.")
        return

    table_exists = spark.catalog.tableExists(table_name)

    if not table_exists:
        logger.info(f"Creating new Delta table: {table_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info(f"Table created with {row_count} rows.")
    else:
        logger.info(f"Merging {row_count} rows into existing table: {table_name}")
        delta_table = DeltaTable.forName(spark, table_name)

        delta_table.alias("target").merge(
            df.alias("source"),
            "target.ticker = source.ticker AND target.Date = source.Date",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info("Merge completed successfully.")

    # Log final stats
    final_count = spark.sql(
        f"SELECT COUNT(*) AS total_rows FROM {table_name}"
    ).collect()[0].total_rows
    ticker_count = spark.sql(
        f"SELECT COUNT(DISTINCT ticker) AS num_tickers FROM {table_name}"
    ).collect()[0].num_tickers
    logger.info(f"Final table stats: {final_count} total rows, {ticker_count} distinct tickers")
