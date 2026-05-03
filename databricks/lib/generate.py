"""Toy data generation. The row-building step is a pure function for
unit-testability; only `build_dataframe` needs Spark.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import List, Tuple


Row = Tuple[int, date, str, float]


def build_rows(job_id: int, start_date: date, end_date: date) -> List[Row]:
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    rows: List[Row] = []
    d = start_date
    while d <= end_date:
        rows.append((job_id, d, "toy_metric", 100.0))
        d += timedelta(days=1)
    return rows


def build_dataframe(spark, job_id: int, start_date: date, end_date: date):
    """Build the result DataFrame, including a server-side `created_at`."""
    from pyspark.sql import functions as F

    from .schema import result_row_schema

    rows = build_rows(job_id, start_date, end_date)
    df = spark.createDataFrame(rows, schema=result_row_schema())
    return df.withColumn("created_at", F.current_timestamp())
