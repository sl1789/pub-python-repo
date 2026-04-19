import time

import sys
import os

# Adds the current directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta, date
from sqlmodel import Session, select
from app.db.session import engine
from app.db.models import Job, JobStatus, ResultRow

from app.runners.factory import get_runner
from app.runners.base import RunnerError
from app.runners.local import LocalRunner



POLL_SECONDS = 1.0
SIMULATED_SECONDS = 5

def mark_running(session: Session, job: Job, external_run_id: str | None , external_output_ref:str | None):
    job.status = JobStatus.RUNNING
    job.started_at = job.started_at or datetime.now()
    job.updated_at = datetime.now()
    if external_run_id:
        job.external_run_id = external_run_id
    job.output_ref= external_output_ref
    session.add(job)
    session.commit()
    session.refresh(job)
    
def apply_poll_result(session: Session, job: Job, status: JobStatus, output_ref: str |None, error_message: str | None):
    job.status = status
    job.updated_at = datetime.now()
    
    if status == JobStatus.SUCCEEDED:
        job.finished_at = datetime.now()
        job.output_ref = output_ref
        job.error_message = None
        
    if status == JobStatus.FAILED:
        job.finished_at = datetime.now()
        job.output_ref = None
        job.error_message = error_message or "Unknown failure"
        
    session.add(job)
    session.commit()
    
def process_queued_job(session: Session, job: Job):
    runner = get_runner(job.runner)
    # Local runner: execute synchronously in worker
    if isinstance(runner, LocalRunner):
        mark_running(session, job, external_run_id=None)
        result = runner.execute(session, job)
        apply_poll_result(session, job, result.status, result.output_ref,
        result.error_message)
        return
        
    # External runner: submit and mark RUNNING
    submit = runner.submit(job_id=job.id, params=job.params)
    mark_running(session, job, external_run_id=submit.external_run_id,external_output_ref=submit.output_ref)
    
def poll_running_jobs(session: Session):
    running_jobs = session.exec(select(Job).where(Job.status ==JobStatus.RUNNING)).all()
    for job in running_jobs:
        try:
            runner = get_runner(job.runner)
            poll = runner.poll(job.external_run_id)
            if poll:
                apply_poll_result(session, job, poll.status, poll.output_ref, poll.error_message)
        except RunnerError as e:
            # Treat runner poll errors as transient; keep job RUNNING but record last error
            job.updated_at = datetime.utcnow()
            job.error_message = f"Poll error: {e}"
            session.add(job)
            session.commit()
            

# def process_job(session: Session, job: Job):
#     # Mark RUNNING
#     job.status = JobStatus.RUNNING
#     job.started_at = datetime.now()
#     job.updated_at = datetime.now()
#     session.add(job)
#     session.commit()
#     session.refresh(job)
    
#     # Simulate work
#     time.sleep(SIMULATED_SECONDS)
    
#     # Produce toy results for this job
#     start_date = date.fromisoformat(job.params["start_date"])
#     end_date = date.fromisoformat(job.params["end_date"])
    
#     d = start_date
#     rows = []
#     while d <= end_date:
#         rows.append(ResultRow(
#             job_id=job.id,
#             business_date=d, 
#             metric_name="toy_metric",
#             metric_value=100.0))
#         d += timedelta(days=1)
        
#     session.add_all(rows)
    
#     # Mark SUCCEEDED
#     job.status = JobStatus.SUCCEEDED
#     job.finished_at = datetime.now()
#     job.updated_at = datetime.now()
#     job.output_ref = f"sqlite:result_rows(job_id={job.id})"
#     job.error_message = None # enforce
#     session.add(job)
#     session.commit()
    
def main():
    print("Worker started (Runner-based). Polling for jobs...")
    while True:
        with Session(engine) as session:
            # 1) Poll external running jobs
            poll_running_jobs(session)
            
            # 2) Pick up one queued job (FIFO)
            job = session.exec(
                select(Job)
                .where(Job.status ==JobStatus.QUEUED)
                .order_by(Job.created_at)
                .limit(1)
                ).first()
            
            if job:
                try:
                    print(f"Processing queued job {job.id} runner={job.runner}")
                    process_queued_job(session, job)
                except Exception as e:
                    apply_poll_result(session, job, JobStatus.FAILED, None, str(e))
                    print(f"Job {job.id} failed: {e}")
            else:
                time.sleep(POLL_SECONDS)
            

if __name__=="__main__":
    main()            