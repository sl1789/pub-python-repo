"""Explicit Spark schema for job result rows.

Imported lazily inside a function so this module remains importable in a
pure-Python (no PySpark) environment for unit testing the rest of `lib`.
"""
from __future__ import annotations


def result_row_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("job_id", T.IntegerType(), nullable=False),
            T.StructField("business_date", T.DateType(), nullable=False),
            T.StructField("metric_name", T.StringType(), nullable=False),
            T.StructField("metric_value", T.DoubleType(), nullable=False),
        ]
    )
