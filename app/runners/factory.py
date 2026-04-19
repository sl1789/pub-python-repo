from future import annotations
from app.runners.base import BaseRunner, RunnerError
from app.runners.local import LocalRunner
from app.runners.databricks import DatabricksRunner
from app.runners.airflow import AirflowRunner

def get_runner(name: str) -> BaseRunner:
    name = (name or "local").lower()
    if name == "local":
        return LocalRunner()
    if name == "databricks":
        return DatabricksRunner()
    if name == "airflow":
        return AirflowRunner()
    raise RunnerError(f"Unknown runner: {name}")