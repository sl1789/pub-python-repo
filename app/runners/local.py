from __future__ import annotations
import time
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional
from sqlmodel import Session
from app.db.models import Job, JobStatus, ResultRow
from app.runners.base import BaseRunner, SubmitResult, PollResult

class LocalRunner(BaseRunner):
    """
    Executes the job locally (in the worker process) and writes results into ResultRow.
    This is the Phase A behavior, now behind a Runner abstraction."""
    
    def init(self, simulated_seconds: int = 3):
        self.simulated_seconds = simulated_seconds
        
    def submit(self, job_id: int, params: Dict[str, Any]) -> SubmitResult:
        # local execution doesn't have an external run id
        return SubmitResult(external_run_id=None)
    
    def poll(self, external_run_id: Optional[str]) -> Optional[PollResult]:
        # local runner doesn't poll an external system
        return None
    
    def execute(self, session: Session, job: Job) -> PollResult:
        """
        Perform the actual compute + write results.
        Called by the worker when job.runner == 'local'.
        """
        time.sleep(self.simulated_seconds)
        start_date = date.fromisoformat(job.params["start_date"])
        end_date = date.fromisoformat(job.params["end_date"])
        
        rows = []
        d = start_date
        while d <= end_date:
            rows.append(ResultRow(job_id=job.id, business_date=d,metric_name="toy_metric", metric_value=100.0))
            d += timedelta(days=1)
            
        session.add_all(rows)
        
        return PollResult(
            status=JobStatus.SUCCEEDED,
            output_ref=f"sqlite:result_rows(job_id={job.id})",
            error_message=None,
            )
        
