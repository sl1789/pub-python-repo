"""Centralized path-building so the notebook, the API runner and the
results reader all agree on the export layout.
"""
from __future__ import annotations
import re

_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")


def build_mc_export_path(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    ticker: str,
) -> str:
    """Return the abfss:// path written by the monte_carlo_simulation notebook.

    Layout:
        abfss://<container>@<storage_account>.dfs.core.windows.net/<prefix>/ticker=<ticker>/
    """
    for name, value in (
        ("storage_account", storage_account),
        ("container", container),
        ("prefix", prefix),
    ):
        if not value or not isinstance(value, str):
            raise ValueError(f"{name} must be a non-empty string")
    if not isinstance(ticker, str) or not _TICKER_RE.match(ticker):
        raise ValueError(f"invalid ticker: {ticker!r}")
    return (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"{prefix}/ticker={ticker}"
    )


def build_mc_output_ref(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    ticker: str,
) -> str:
    """`output_ref` value persisted on a Monte Carlo Job row."""
    return "parquet:" + build_mc_export_path(
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        ticker=ticker,
    )

