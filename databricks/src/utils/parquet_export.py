"""Pandas -> Parquet helpers for notebook exports.

Each Databricks notebook persists primary results to a Delta table for
analyst consumption, and a parquet snapshot to ADLS for the FastAPI service
to read. Both writes share the same `dataset` taxonomy defined in
`databricks/lib/paths.py`.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from config.settings import get_export_path, get_logger

logger = get_logger(__name__)


def export_pandas_to_parquet(
    pdf: pd.DataFrame,
    *,
    dataset: str,
    ticker: str | None = None,
    partition_by_ticker: bool = False,
) -> None:
    """Write `pdf` to the dataset's ADLS folder as parquet.

    For ticker-partitioned datasets the caller can either:
      * pass `ticker=<TICKER>` to write that ticker's partition directly, or
      * pass `partition_by_ticker=True` and let this helper iterate over
        ``pdf["ticker"].unique()`` and write each partition.

    The write is a single-file overwrite per partition (small datasets only;
    these are aggregated analyst outputs, not raw event logs).

    Gracefully no-ops with a warning if Azure storage is not configured, so
    notebooks can still run end-to-end on a workstation without ADLS access.
    """
    if pdf is None or pdf.empty:
        logger.warning(f"export_pandas_to_parquet skipped: empty DataFrame (dataset={dataset})")
        return

    try:
        if partition_by_ticker:
            if "ticker" not in pdf.columns:
                raise ValueError("partition_by_ticker=True requires a 'ticker' column")
            tickers: Iterable[str] = sorted(pdf["ticker"].unique())
            for t in tickers:
                path = get_export_path(dataset, t)
                pdf[pdf["ticker"] == t].to_parquet(path, index=False)
                logger.info(f"[OK] parquet_export dataset={dataset} ticker={t} -> {path}")
            return

        path = get_export_path(dataset, ticker)
        pdf.to_parquet(path, index=False)
        logger.info(f"[OK] parquet_export dataset={dataset} ticker={ticker} -> {path}")

    except RuntimeError as e:
        if "AZURE_STORAGE_ACCOUNT" in str(e):
            logger.warning(
                f"Parquet export skipped for dataset={dataset} (no Azure storage configured)"
            )
            return
        raise
