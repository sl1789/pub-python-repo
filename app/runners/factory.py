from __future__ import annotations
from app.runners.base import BaseRunner, RunnerError
from app.runners.databricks import DatabricksRunner


def get_runner(name: str) -> BaseRunner:
    name = (name or "").lower()
    if name == "databricks":
        return DatabricksRunner()
    raise RunnerError(f"Unknown runner: {name}")
