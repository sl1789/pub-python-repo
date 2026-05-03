"""Centralized path-building so the notebook, the API runner and the
results reader all agree on the export layout.
"""
from __future__ import annotations


def build_export_path(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    job_id: int,
) -> str:
    """Return the abfss:// path for a given job's parquet snapshot.

    Layout:
        abfss://<container>@<storage_account>.dfs.core.windows.net/<prefix>/job_id=<job_id>/
    """
    if not isinstance(job_id, int):
        raise TypeError(f"job_id must be int, got {type(job_id).__name__}")
    if job_id <= 0:
        raise ValueError(f"job_id must be > 0, got {job_id}")
    for name, value in (
        ("storage_account", storage_account),
        ("container", container),
        ("prefix", prefix),
    ):
        if not value or not isinstance(value, str):
            raise ValueError(f"{name} must be a non-empty string")
    return (
        f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        f"{prefix}/job_id={job_id}"
    )


def build_output_ref(
    *,
    storage_account: str,
    container: str,
    prefix: str,
    job_id: int,
) -> str:
    """`output_ref` value persisted on the Job row (parquet:<abfss path>)."""
    return "parquet:" + build_export_path(
        storage_account=storage_account,
        container=container,
        prefix=prefix,
        job_id=job_id,
    )
