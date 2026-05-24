"""HTTP routes for browsing parquet datasets emitted by Databricks notebooks.

Each dataset is a folder under `<container>/<prefix>/<dataset>/` on ADLS.
`GET /datasets` lists them; `GET /datasets/{name}` reads rows from one,
optionally filtered to a single ticker partition.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

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
    rows: List[Dict[str, Any]]


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
        output_ref = build_dataset_output_ref(
            storage_account=AZURE_STORAGE_ACCOUNT,
            container=AZURE_RESULTS_CONTAINER,
            prefix=AZURE_RESULTS_PREFIX,
            dataset=spec.name,
            ticker=ticker,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    # Strip the `parquet:` scheme since AzureParquetResultsRepository wants the
    # raw abfss:// path.
    path = output_ref[len("parquet:"):]
    try:
        repo = AzureParquetResultsRepository(path=path)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    try:
        # No filters: dataset endpoints return whatever is in the partition.
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
        logger.exception("read_dataset failed for dataset=%s ticker=%s", name, ticker)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load dataset: {type(e).__name__}: {e}",
        ) from e

    return DatasetRowsResponse(dataset=name, ticker=ticker, rows=rows)
