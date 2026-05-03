"""End-to-end job orchestration. Imported by both the `.py` and `.ipynb`
entrypoints so there is exactly one source of truth.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Dict

from .generate import build_dataframe
from .params import parse_params
from .paths import build_export_path
from .sinks import export_parquet_snapshot, write_delta_partition


def run_job(spark, widget_getter: Callable[[str], str]) -> Dict[str, Any]:
    """Execute the full job and return a JSON-serializable summary."""
    # Standardize timestamps; matches the rest of the platform (UTC).
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    params = parse_params(widget_getter)

    df = build_dataframe(
        spark,
        job_id=params.job_id,
        start_date=params.start_date,
        end_date=params.end_date,
    )

    fq_table = write_delta_partition(
        df,
        database=params.database,
        table=params.table,
        job_id=params.job_id,
    )

    export_path = build_export_path(
        storage_account=params.storage_account,
        container=params.container,
        prefix=params.prefix,
        job_id=params.job_id,
    )
    export_parquet_snapshot(df, export_path=export_path)

    return {
        "job_id": params.job_id,
        "delta_table": fq_table,
        "parquet_export": export_path,
        "rows": df.count(),
    }


def run_and_exit(spark, dbutils) -> None:
    """Notebook entrypoint: run the job and exit with a JSON summary."""
    summary = run_job(spark, dbutils.widgets.get)
    dbutils.notebook.exit(json.dumps(summary))
