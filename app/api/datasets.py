"""HTTP routes for browsing parquet datasets emitted by Databricks notebooks.

Each dataset is a folder under `<container>/<prefix>/<dataset>/` on ADLS.
`GET /datasets` lists them; `GET /datasets/{name}` reads rows from one,
optionally filtered to a single ticker partition.

The `mc_vs_actual_test.ipynb` notebook stamps a `created_at` column on
every row before writing parquet. We surface ``max(created_at)`` as
``ds_updated_at`` so the UI can show a "data as of …" badge without a
second round-trip.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.core.config import (
    AZURE_RESULTS_CONTAINER,
    AZURE_RESULTS_PREFIX,
    AZURE_STORAGE_ACCOUNT,
)
from app.core.security import require_roles
from app.results.datasets import DATASETS
from app.results.parquet_azure import AzureParquetResultsRepository
from databricks.lib.paths import build_dataset_output_ref

router = APIRouter(prefix="/datasets", tags=["datasets"])
logger = logging.getLogger(__name__)


class DatasetInfo(BaseModel):
    name: str
    description: str
    ticker_partitioned: bool


class DatasetListResponse(BaseModel):
    items: List[DatasetInfo]


class DatasetRowsResponse(BaseModel):
    dataset: str
    ticker: Optional[str] = None
    ds_updated_at: Optional[str] = None
    rows: List[Dict[str, Any]]


class EmcJoinRow(BaseModel):
    method: str
    side: str  # "call" or "put"
    pre_mape: float
    post_mape: float
    delta: float  # post_mape - pre_mape (negative = EMC improves accuracy)


class EmcJoinResponse(BaseModel):
    ticker: str
    num_runs: Optional[int] = None
    ds_updated_at: Optional[str] = None
    rows: List[EmcJoinRow]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_path(dataset: str, ticker: Optional[str]) -> str:
    """Construct the abfss:// path for a (dataset, ticker) and strip the
    `parquet:` scheme since the repository wants the raw abfss URI.
    """
    output_ref = build_dataset_output_ref(
        storage_account=AZURE_STORAGE_ACCOUNT,
        container=AZURE_RESULTS_CONTAINER,
        prefix=AZURE_RESULTS_PREFIX,
        dataset=dataset,
        ticker=ticker,
    )
    return output_ref[len("parquet:"):]


def _load_parquet_df(path: str) -> pd.DataFrame:
    """Construct the repo and return the raw DataFrame.

    Centralised so route handlers can re-use the same error mapping
    (404 / 502 / 503 / 500). The repo applies no filters because
    the dataset endpoints don't take per-row query params.
    """
    try:
        repo = AzureParquetResultsRepository(path=path)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    try:
        rows = repo.load_results(params={})
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"No parquet found at {path}: {e}",
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Storage auth failed: {e}",
        ) from e
    except Exception as e:
        logger.exception("parquet load failed for %s", path)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load parquet: {type(e).__name__}: {e}",
        ) from e
    return pd.DataFrame(rows)


def _ds_updated_at(df: pd.DataFrame) -> Optional[str]:
    """Return the ISO8601 ``max(created_at)`` or None when absent.

    The mc_vs_actual_test notebook stamps `created_at` on every row before
    the parquet export. We tolerate older data without the column.
    """
    if "created_at" not in df.columns or df.empty:
        return None
    try:
        ts = pd.to_datetime(df["created_at"]).max()
        return ts.isoformat() if pd.notna(ts) else None
    except Exception:
        return None


def _mape(df: pd.DataFrame, side: str) -> pd.Series:
    """MAPE-by-method for one option side.

    Mirrors the helper used inline in the notebook so the API returns the
    same numbers the analyst sees in matplotlib.
    """
    actual_col = f"actual_{side}"
    mc_col = f"mc_{side}"
    if actual_col not in df.columns or mc_col not in df.columns:
        return pd.Series(dtype=float)
    d = df[df[actual_col].notna() & (df[actual_col] > 0.5)].copy()
    if d.empty:
        return pd.Series(dtype=float)
    d["abs_pct"] = ((d[mc_col] - d[actual_col]) / d[actual_col]).abs() * 100
    return d.groupby("method")["abs_pct"].mean()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("", response_model=DatasetListResponse, dependencies=[Depends(require_roles("viewer"))])
def list_datasets():
    return DatasetListResponse(
        items=[
            DatasetInfo(
                name=spec.name,
                description=spec.description,
                ticker_partitioned=spec.ticker_partitioned,
            )
            for spec in DATASETS.values()
        ]
    )


@router.get(
    "/mc_vs_actual_emc_join",
    response_model=EmcJoinResponse,
    dependencies=[Depends(require_roles("viewer"))],
)
def emc_pre_post_join(
    ticker: str = Query(..., description="Ticker partition to read (required)"),
):
    """Compute pre/post-EMC MAPE deltas server-side.

    The mc_vs_actual_test notebook writes the EMC-corrected results to
    the ``emc_diagnostics`` dataset and the pre-EMC results into
    ``mc_vs_actual`` (at ``num_runs == emc_scale``). The two folders
    have to be JOINed on (method, K, T) to produce the pre vs post bar
    chart the notebook draws inline. Centralising the join here keeps
    the UI thin and means every client (Streamlit, notebook, future SPA)
    gets the same numbers.
    """
    try:
        emc_path = _build_path("emc_diagnostics", ticker)
        mva_path = _build_path("mc_vs_actual", ticker)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    emc_df = _load_parquet_df(emc_path)
    mva_df = _load_parquet_df(mva_path)

    if emc_df.empty or mva_df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Empty inputs for ticker={ticker}: "
                   f"emc_rows={len(emc_df)}, mc_vs_actual_rows={len(mva_df)}",
        )

    # The EMC notebook re-runs at a single scale (`emc_scale`) and stamps it
    # in `num_runs`. Filter the pre-EMC data to that scale and to the same
    # methods EMC was applied to.
    try:
        emc_scale = int(emc_df["num_runs"].iloc[0])
    except Exception:
        emc_scale = int(mva_df["num_runs"].max())

    methods = sorted(emc_df["method"].unique())
    pre = mva_df[
        (mva_df["num_runs"] == emc_scale) & mva_df["method"].isin(methods)
    ]

    rows: List[EmcJoinRow] = []
    for side in ("call", "put"):
        pre_m = _mape(pre, side)
        post_m = _mape(emc_df, side)
        for method in methods:
            if method not in pre_m.index or method not in post_m.index:
                continue
            pre_v = float(pre_m[method])
            post_v = float(post_m[method])
            rows.append(EmcJoinRow(
                method=method,
                side=side,
                pre_mape=round(pre_v, 4),
                post_mape=round(post_v, 4),
                delta=round(post_v - pre_v, 4),
            ))

    # Use whichever DataFrame has the freshest stamp.
    updated = max(
        (s for s in (_ds_updated_at(emc_df), _ds_updated_at(mva_df)) if s),
        default=None,
    )
    return EmcJoinResponse(
        ticker=ticker,
        num_runs=emc_scale,
        ds_updated_at=updated,
        rows=rows,
    )


@router.get(
    "/{name}",
    response_model=DatasetRowsResponse,
    dependencies=[Depends(require_roles("viewer"))],
)
def read_dataset(
    name: str,
    ticker: Optional[str] = Query(default=None),
):
    spec = DATASETS.get(name)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {name!r}")

    if spec.ticker_partitioned and not ticker:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset {name!r} is ticker-partitioned; a `ticker` query param is required.",
        )
    if not spec.ticker_partitioned and ticker:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset {name!r} is not ticker-partitioned; remove the `ticker` query param.",
        )

    try:
        path = _build_path(spec.name, ticker)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    df = _load_parquet_df(path)
    return DatasetRowsResponse(
        dataset=name,
        ticker=ticker,
        ds_updated_at=_ds_updated_at(df),
        rows=df.to_dict(orient="records"),
    )
