"""Widget parsing and validation for the Databricks job entrypoint.

These helpers are pure Python so they can be unit-tested without a Spark
session or a Databricks runtime.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable, Mapping


@dataclass(frozen=True)
class JobParams:
    job_id: int
    start_date: date
    end_date: date
    storage_account: str
    container: str
    prefix: str
    database: str
    table: str


def parse_params(getter: Callable[[str], str]) -> JobParams:
    """Parse and validate widget values via a string getter.

    `getter` is typically `dbutils.widgets.get`, but tests can pass a dict
    lookup. Raises `ValueError` on any invalid input so the job fails fast.
    """
    raw = {
        "job_id": getter("job_id"),
        "start_date": getter("start_date"),
        "end_date": getter("end_date"),
        "storage_account": getter("storage_account"),
        "container": getter("container"),
        "prefix": getter("prefix"),
        "database": getter("database"),
        "table": getter("table"),
    }

    try:
        job_id = int(raw["job_id"])
    except (TypeError, ValueError) as e:
        raise ValueError(f"job_id must be an integer, got {raw['job_id']!r}") from e
    if job_id <= 0:
        raise ValueError(f"job_id must be > 0, got {job_id}")

    try:
        start_date = date.fromisoformat(raw["start_date"])
        end_date = date.fromisoformat(raw["end_date"])
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"start_date/end_date must be ISO YYYY-MM-DD; got "
            f"{raw['start_date']!r}/{raw['end_date']!r}"
        ) from e
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date}) must be <= end_date ({end_date})"
        )

    storage_account = (raw["storage_account"] or "").strip()
    container = (raw["container"] or "").strip()
    prefix = (raw["prefix"] or "").strip()
    database = (raw["database"] or "").strip()
    table = (raw["table"] or "").strip()

    for name, value in (
        ("storage_account", storage_account),
        ("container", container),
        ("prefix", prefix),
        ("database", database),
        ("table", table),
    ):
        if not value:
            raise ValueError(f"{name} widget is required and cannot be empty")

    return JobParams(
        job_id=job_id,
        start_date=start_date,
        end_date=end_date,
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        database=database,
        table=table,
    )


def parse_params_from_mapping(values: Mapping[str, str]) -> JobParams:
    """Convenience wrapper used in tests."""
    return parse_params(lambda k: values.get(k, ""))
