import time

import sys
import os

# Adds the current directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta, date
from sqlmodel import Session, select
from app.db.session import engine
from app.db.models import Job, JobStatus, ResultRow

POLL_SECONDS = 1.0
SIMULATED_SECONDS = 5

def process_job(session: Session, job: Job):
    # Mark RUNNING
    job.status = JobStatus.RUNNING
    job.started_at = datetime.now()
    job.updated_at = datetime.now()
    session.add(job)
    session.commit()
    session.refresh(job)
    
    # Simulate work
    time.sleep(SIMULATED_SECONDS)
    
    # Produce toy results for this job
    start_date = date.fromisoformat(job.params["start_date"])
    end_date = date.fromisoformat(job.params["end_date"])
    
    d = start_date
    rows = []
    while d <= end_date:
        rows.append(ResultRow(
            job_id=job.id,
            business_date=d, 
            metric_name="toy_metric",
            metric_value=100.0))
        d += timedelta(days=1)
        
    session.add_all(rows)
    
    # Mark SUCCEEDED
    job.status = JobStatus.SUCCEEDED
    job.finished_at = datetime.now()
    job.updated_at = datetime.now()
    job.output_ref = f"sqlite:result_rows(job_id={job.id})"
    job.error_message = None # enforce
    session.add(job)
    session.commit()
    
def main():
    print("Worker started. Polling for QUEUED jobs...")
    while True:
        with Session(engine) as session:
            job = session.exec(
                select(Job)
                .where(Job.status == JobStatus.QUEUED)
                .order_by(Job.created_at)
                .limit(1)
            ).first()
            
            if job:
                try:
                    print(f"Processing job {job.id}...")
                    process_job(session, job)
                    print(f"Job {job.id} done.")
                except Exception as e:
                    job.status = JobStatus.FAILED
                    job.error_message = str(e)
                    job.finished_at = datetime.now()
                    job.updated_at = datetime.now()
                    job.output_ref = None
                    session.add(job)
                    session.commit()
                    print(f"Job {job.id} failed: {e}")
            else:
                time.sleep(POLL_SECONDS)

if __name__=="__main__":
    main()            