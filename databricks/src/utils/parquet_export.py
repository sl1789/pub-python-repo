"""Pandas -> Parquet helpers for notebook exports (via Spark write).

Each Databricks notebook persists primary results to a Delta table for
analyst consumption (APPEND mode with run_id for full history), and a
parquet snapshot to ADLS for the FastAPI service to read (OVERWRITE,
latest only).

The parquet path matches what the API reader expects:
    abfss://<container>@<account>.dfs.core.windows.net/<prefix>/<dataset>/[ticker=<TICKER>]/

The API calls pd.read_parquet(directory_path) which reads all .parquet
files inside. We write a single coalesced file per partition using Spark's
native parquet writer (which uses the spark.conf ADLS auth — no separate
storage_options needed).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from config.settings import get_export_path, get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


def _get_spark() -> "SparkSession":
    """Get the active SparkSession (always available in Databricks)."""
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def export_pandas_to_parquet(
    pdf: pd.DataFrame,
    *,
    dataset: str,
    ticker: str | None = None,
    partition_by_ticker: bool = False,
) -> None:
    """Write `pdf` to the dataset's ADLS folder as parquet via Spark.

    Uses Spark's native parquet writer so that existing
    spark.conf ADLS authentication applies — no separate adlfs/storage_options.

    For ticker-partitioned datasets the caller can either:
      * pass `ticker=<TICKER>` to write that ticker's partition directly, or
      * pass `partition_by_ticker=True` and let this helper iterate over
        ``pdf["ticker"].unique()`` and write each partition.

    Each call OVERWRITES the previous snapshot (the API always shows the
    latest results). Historical data is preserved in Delta tables.

    Gracefully no-ops with a warning if Azure storage is not configured, so
    notebooks can still run end-to-end on a workstation without ADLS access.
    """
    if pdf is None or pdf.empty:
        logger.warning(f"export_pandas_to_parquet skipped: empty DataFrame (dataset={dataset})")
        return

    try:
        spark = _get_spark()

        if partition_by_ticker:
            if "ticker" not in pdf.columns:
                raise ValueError("partition_by_ticker=True requires a 'ticker' column")
            tickers = sorted(pdf["ticker"].unique())
            for t in tickers:
                dir_path = get_export_path(dataset, t)
                partition_pdf = pdf[pdf["ticker"] == t]
                sdf = spark.createDataFrame(partition_pdf)
                (
                    sdf.coalesce(1)
                    .write.mode("overwrite")
                    .parquet(dir_path)
                )
                logger.info(f"[OK] parquet export: {dataset}/ticker={t} -> {dir_path}")
            return

        # Non-partitioned write (e.g. scalability)
        dir_path = get_export_path(dataset, ticker)
        sdf = spark.createDataFrame(pdf)
        (
            sdf.coalesce(1)
            .write.mode("overwrite")
            .parquet(dir_path)
        )
        logger.info(f"[OK] parquet export: {dataset} -> {dir_path}")

    except RuntimeError as e:
        if "AZURE_STORAGE_ACCOUNT" in str(e):
            logger.warning(
                f"Parquet export skipped for dataset={dataset} (no Azure storage configured)"
            )
            return
        raise
