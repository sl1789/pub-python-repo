"""Write side: Delta upsert by partition + parquet snapshot export.

Both functions write atomically with respect to the `job_id` partition so
re-runs are idempotent and never clobber data from other jobs.
"""
from __future__ import annotations


def write_delta_partition(df, *, database: str, table: str, job_id: int) -> str:
    """Overwrite only the rows for this job_id in the Delta table.

    Uses `replaceWhere` so a single job run never touches other partitions,
    even if `partitionOverwriteMode` isn't set on the cluster.
    """
    spark = df.sparkSession
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
    fq_table = f"{database}.{table}"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"job_id = {int(job_id)}")
        .partitionBy("job_id")
        .saveAsTable(fq_table)
    )
    return fq_table


def export_parquet_snapshot(df, *, export_path: str) -> str:
    """Write the parquet snapshot consumed by the API.

    The dataframe is written with `mode=overwrite`; the `_SUCCESS` marker
    serves as the readers' completion signal.
    """
    df.drop("created_at").write.mode("overwrite").parquet(export_path)
    return export_path
