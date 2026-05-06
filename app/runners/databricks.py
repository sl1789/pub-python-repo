from __future__ import annotations
import requests
from typing import Dict, Any, Optional
from app.db.models import JobStatus
from app.runners.base import BaseRunner, RunnerError, SubmitResult, PollResult
from app.core.config import (
    AZURE_STORAGE_ACCOUNT,
    AZURE_RESULTS_CONTAINER,
    AZURE_MC_RESULTS_PREFIX,
)
from app.core.config import (
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    DATABRICKS_MC_JOB_ID,
)
from databricks.lib.paths import build_mc_output_ref


class DatabricksRunner(BaseRunner):
    """
    Databricks Jobs runner for the `monte_carlo_simulation` notebook.

    Required env vars: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_MC_JOB_ID.
    """

    def __init__(self):
        if not (DATABRICKS_HOST and DATABRICKS_TOKEN and DATABRICKS_MC_JOB_ID):
            raise RunnerError("Missing DATABRICKS_HOST/TOKEN/MC_JOB_ID")
        self.host = DATABRICKS_HOST
        self.token = DATABRICKS_TOKEN
        self.job_id = int(DATABRICKS_MC_JOB_ID)

    def submit(self, job_id: int, params: Dict[str, Any]) -> SubmitResult:
        ticker = params["ticker"]
        payload = {
            "job_id": self.job_id,
            "job_parameters": {
                # Widget names mirror monte_carlo_simulation.ipynb:
                #   ticker, target_value, period, num_simulations
                "ticker": str(ticker),
                "target_value": str(params["strike"]),
                "period": str(params["period_days"]),
                "num_simulations": str(params["num_simulations"]),
            },
        }
        run_id = self._run_now(payload)
        output_ref = build_mc_output_ref(
            storage_account=AZURE_STORAGE_ACCOUNT,
            container=AZURE_RESULTS_CONTAINER,
            prefix=AZURE_MC_RESULTS_PREFIX,
            ticker=ticker,
        )
        return SubmitResult(external_run_id=str(run_id), output_ref=output_ref)

    def poll(self, external_run_id: Optional[str]) -> Optional[PollResult]:
        if not external_run_id:
            return None

        url = f"{self.host}/api/2.0/jobs/runs/get"
        headers = {"Authorization": f"Bearer {self.token}"}
        r = requests.get(url, headers=headers, params={"run_id": external_run_id}, timeout=30)
        if r.status_code >= 300:
            raise RunnerError(f"Databricks runs/get failed: {r.status_code} {r.text}")

        data = r.json()
        state = data.get("state") or {}
        life = (state.get("life_cycle_state") or "").upper()
        result = (state.get("result_state") or "").upper()
        message = state.get("state_message")

        if life in {"PENDING", "RUNNING", "TERMINATING"}:
            return PollResult(status=JobStatus.RUNNING)
        if life == "TERMINATED":
            if result == "SUCCESS":
                # output_ref is set at submit time; do not overwrite here.
                return PollResult(status=JobStatus.SUCCEEDED)
            return PollResult(
                status=JobStatus.FAILED,
                error_message=message or f"Databricks failed: {result}",
            )
        return None

    def cancel(self, external_run_id: str):
        url = f"{self.host}/api/2.0/jobs/runs/cancel"
        headers = {"Authorization": f"Bearer {self.token}"}
        requests.post(
            url,
            headers=headers,
            json={"run_id": int(external_run_id)},
            timeout=10,
        )

    def _run_now(self, payload: Dict[str, Any]) -> str:
        url = f"{self.host}/api/2.0/jobs/run-now"
        headers = {"Authorization": f"Bearer {self.token}"}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code >= 300:
            raise RunnerError(f"Databricks run-now failed: {r.status_code} {r.text}")
        data = r.json()
        run_id = data.get("run_id") or data.get("runId")
        if not run_id:
            raise RunnerError(f"Databricks response missing run_id: {data}")
        return str(run_id)
