"""Registry of parquet datasets exposed by the API.

Each entry mirrors a folder produced by `databricks/jobs/mc_vs_actual_test.ipynb`.
Centralising the list here means the API and UI agree on what is queryable
without each route hard-coding paths.

Scope: this endpoint surfaces the *research* outputs from
``mc_vs_actual_test``. The per-job ``simulations`` parquet is intentionally
NOT listed here — that flow is owned by ``/results?job_id=`` so callers
inspect a single MC run by its job id, not by browsing the whole prefix.
The ``options`` (yfinance snapshots) and ``scalability`` (benchmark)
datasets are likewise out of scope until they have a dedicated UI.
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
}
