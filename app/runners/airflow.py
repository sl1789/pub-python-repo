from future import annotations
import os
import requests
from typing import Dict, Any, Optional
from app.db.models import JobStatus
from app.runners.base import BaseRunner, RunnerError, SubmitResult, PollResult

class AirflowRunner(BaseRunner):
    """
    Minimal Airflow REST runner.
    Env vars:
    AIRFLOW_BASE_URL e.g. https://airflow.mycorp.com
    AIRFLOW_DAG_ID
    AIRFLOW_USER
    AIRFLOW_PASSWORD
    """
    
    def init(self):
        self.base_url = os.getenv("AIRFLOW_BASE_URL")
        self.dag_id = os.getenv("AIRFLOW_DAG_ID")
        self.user = os.getenv("AIRFLOW_USER")
        self.password = os.getenv("AIRFLOW_PASSWORD")
        
        if not (self.base_url and self.dag_id and self.user and self.password):
            raise RunnerError("Missing AIRFLOW_BASE_URL/AIRFLOW_DAG_ID/AIRFLOW_USER/AIRFLOW_PASSWORD")
        
    def submit(self, job_id: int, params: Dict[str, Any]) -> SubmitResult:
        url = f"{self.base_url}/api/v1/dags/{self.dag_id}/dagRuns"
        dag_run_id = f"ui_{job_id}"
        
        payload = {
            "dag_run_id": dag_run_id,
            "conf": {**params, "job_id": job_id},"note": "Triggered from API",
        }
        
        r = requests.post(url, auth=(self.user, self.password), json=payload, timeout=30)
        if r.status_code >= 300:
            raise RunnerError(f"Airflow trigger failed: {r.status_code} {r.text}")
        
        return SubmitResult(external_run_id=dag_run_id)
    
    def poll(self, external_run_id: Optional[str]) -> Optional[PollResult]:
        if not external_run_id:
            return None
        
        url = f"{self.base_url}/api/v1/dags/{self.dag_id}/dagRuns/{external_run_id}"
        r = requests.get(url, auth=(self.user, self.password), timeout=30)
        
        if r.status_code >= 300:
            raise RunnerError(f"Airflow dagRun get failed: {r.status_code} {r.text}")
        data = r.json()
        state = (data.get("state") or "").lower()
        if state in {"queued", "running"}:
            return PollResult(status=JobStatus.RUNNING)
        if state == "success":
            return PollResult(status=JobStatus.SUCCEEDED,output_ref=f"airflow:dag_run_id={external_run_id}")
            
        if state == "failed":
            return PollResult(status=JobStatus.FAILED, error_message="Airflow DAG run failed")
        
        return None
    
    