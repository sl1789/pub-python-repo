from __future__ import annotations
import os
import requests
from typing import Dict, Any, Optional
from app.db.models import JobStatus
from app.runners.base import BaseRunner, RunnerError, SubmitResult, PollResult
from app.core.config import AZURE_STORAGE_ACCOUNT, AZURE_RESULTS_CONTAINER,AZURE_RESULTS_PREFIX, AZURE_STORAGE_KEY
from app.core.config import DATABRICKS_HOST, DATABRICKS_TOKEN,DATABRICKS_JOB_ID
from databricks.lib.paths import build_output_ref

class DatabricksRunner(BaseRunner):
    """
    Minimal Databricks Jobs runner.Env vars required:
    DATABRICKS_HOST e.g. https://adb-xxx.azuredatabricks.net
    DATABRICKS_TOKEN PAT token
    DATABRICKS_JOB_ID existing job id
    """
    
    def __init__(self):
        if not (DATABRICKS_HOST and DATABRICKS_TOKEN and DATABRICKS_JOB_ID):
            raise RunnerError("Missing DATABRICKS_HOST/TOKEN/JOB_ID")
        self.host = DATABRICKS_HOST
        self.token = DATABRICKS_TOKEN
        self.job_id = int(DATABRICKS_JOB_ID)

    def _output_ref_for_job(self, job_id: int) -> str:
        # Single source of truth for the export path layout.
        return build_output_ref(
            storage_account=AZURE_STORAGE_ACCOUNT,
            container=AZURE_RESULTS_CONTAINER,
            prefix=AZURE_RESULTS_PREFIX,
            job_id=job_id,
        )
        
    def submit(self, job_id: int, params: Dict[str, Any]) -> SubmitResult:
        url = f"{self.host}/api/2.0/jobs/run-now"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        payload = {
            "job_id": int(self.job_id),
            "job_parameters": {
            **{k: str(v) for k, v in params.items()},
            "job_id": str(job_id), # correlate back to our metadata DB
            "start_date": str(params["start_date"]),
            "end_date": str(params["end_date"]),
            # Notebook layout configuration (kept in sync with the API
            # results reader via the shared paths module).
            "storage_account": AZURE_STORAGE_ACCOUNT,
            "container": AZURE_RESULTS_CONTAINER,
            "prefix": AZURE_RESULTS_PREFIX,
            "database": os.getenv("DATABRICKS_DATABASE", "demo"),
            "table": os.getenv("DATABRICKS_TABLE", "job_results"),
            },
        }

        r = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if r.status_code >= 300:
            raise RunnerError(f"Databricks run-now failed: {r.status_code} {r.text}")
        
        data = r.json()
        run_id = data.get("run_id") or data.get("runId")
        if not run_id:
            raise RunnerError(f"Databricks response missing run_id: {data}")
        
        return SubmitResult(external_run_id=str(run_id),output_ref=self._output_ref_for_job(job_id))
            
    def poll(self, external_run_id: Optional[str]) -> Optional[PollResult]:
        if not external_run_id:
            return None
        
        url = f"{self.host}/api/2.0/jobs/runs/get"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        r = requests.get(url, headers=headers, params={"run_id": external_run_id},
        timeout=30)
        
        if r.status_code >= 300:
            raise RunnerError(f"Databricks runs/get failed: {r.status_code} {r.text}")
        
        data = r.json()
        state = data.get("state") or {}
        life = (state.get("life_cycle_state") or "").upper()
        result = (state.get("result_state") or "").upper()
        message = state.get("state_message")
        
        # Map to our JobStatus
        if life in {"PENDING", "RUNNING", "TERMINATING"}:
            return PollResult(status=JobStatus.RUNNING)
        
        if life == "TERMINATED":
            if result == "SUCCESS":
                # In real life, you'd point output_ref to a table/path written by the job
                return PollResult(status=JobStatus.SUCCEEDED,
                    output_ref=f"parquet:job_id={external_run_id}")
                
            return PollResult(status=JobStatus.FAILED, error_message=message or
                f"Databricks failed: {result}")
        
        # Unknown state
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