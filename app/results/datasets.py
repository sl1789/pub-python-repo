"""Registry of parquet datasets exposed by the API.

Each entry mirrors a folder produced by one of the Databricks notebooks.
Centralising the list here means the API and UI agree on what is queryable
without each route hard-coding paths.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    description: str
    # When True the dataset is partitioned by `ticker=<TICKER>/` and callers
    # must supply a ticker. When False the dataset is a single folder.
    ticker_partitioned: bool


DATASETS: dict[str, DatasetSpec] = {
    "simulations": DatasetSpec(
        name="simulations",
        description="MC option prices per (ticker, K, T, method).",
        ticker_partitioned=True,
    ),
    "mc_vs_actual": DatasetSpec(
        name="mc_vs_actual",
        description="MC vs market option-chain comparison.",
        ticker_partitioned=True,
    ),
    "emc_diagnostics": DatasetSpec(
        name="emc_diagnostics",
        description="EMC pre/post martingale-correction diagnostics.",
        ticker_partitioned=True,
    ),
    "block_length_sweep": DatasetSpec(
        name="block_length_sweep",
        description="Block-bootstrap block-length sensitivity sweep.",
        ticker_partitioned=True,
    ),
    "lam_sweep": DatasetSpec(
        name="lam_sweep",
        description="Multifractal lam parameter sweep.",
        ticker_partitioned=True,
    ),
    "scalability": DatasetSpec(
        name="scalability",
        description="MC scalability benchmark (price/SE vs num_runs).",
        ticker_partitioned=False,
    ),
    "options": DatasetSpec(
        name="options",
        description="Raw option-chain snapshots fetched from yfinance.",
        ticker_partitioned=True,
    ),
}
