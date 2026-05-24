"""Centralized path-building so notebooks, the API runner and the
results reader all agree on the parquet export layout.

Layout (one folder per dataset under the configured prefix):

    <container>/<prefix>/<dataset>/[ticker=<TICKER>/]<parquet files>

Datasets used by this project:
    simulations          MC option prices per (ticker, K, T, method)
    mc_vs_actual         MC vs market option-chain comparison
    emc_diagnostics      EMC pre/post diagnostics sweep
    block_length_sweep   block-bootstrap block-length sweep
    lam_sweep            multifractal lam sweep
    scalability          MC scalability benchmark (flat, no ticker partition)
    options              raw option-chain snapshots from yfinance
"""
from __future__ import annotations
import re

_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")
_DATASET_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")


def build_dataset_export_path(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    dataset: str,
    ticker: str | None = None,
) -> str:
    """Return the abfss:// path for a parquet dataset.

    If `ticker` is None the path is unpartitioned (e.g. ``scalability``).
    """
    for name, value in (
        ("storage_account", storage_account),
        ("container", container),
        ("prefix", prefix),
        ("dataset", dataset),
    ):
        if not value or not isinstance(value, str):
            raise ValueError(f"{name} must be a non-empty string")
    if not _DATASET_RE.match(dataset):
        raise ValueError(f"invalid dataset name: {dataset!r}")
    base = (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"{prefix}/{dataset}"
    )
    if ticker is None:
        return base
    if not isinstance(ticker, str) or not _TICKER_RE.match(ticker):
        raise ValueError(f"invalid ticker: {ticker!r}")
    return f"{base}/ticker={ticker}"


def build_dataset_output_ref(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    dataset: str,
    ticker: str | None = None,
) -> str:
    """`output_ref` (``parquet:abfss://...``) for a parquet dataset."""
    return "parquet:" + build_dataset_export_path(
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        dataset=dataset,
        ticker=ticker,
    )


# ---------------------------------------------------------------------------
# Backwards-compatible wrappers used by older callers (kept thin so we don't
# duplicate validation). New code should call `build_dataset_*` directly.
# ---------------------------------------------------------------------------

def build_mc_export_path(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    ticker: str,
) -> str:
    return build_dataset_export_path(
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        dataset="simulations",
        ticker=ticker,
    )


def build_mc_output_ref(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    ticker: str,
) -> str:
    return build_dataset_output_ref(
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        dataset="simulations",
        ticker=ticker,
    )

