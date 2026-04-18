import time
from datetime import date, timedelta
import pytest

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from app.main import app
from app.db.session import engine
from app.db.models import Job, JobStatus, ResultRow
from worker.worker import process_job

@pytest.fixture
def client(): 
    return TestClient(app)

def test_job_lifecycle_end_to_end(client):
    # 1. Submit a job
    start_date= date.today() - timedelta(days=3)
    end_date=date.today()
    
    payload = {
        "start_date":start_date.isoformat(),
        "end_date":end_date.isoformat(),
        "filters": {"country":"GR"}
    }
    
    r = client.post("/jobs",json=payload)
    assert r.status_code == 202
    
    job_id = r.json()["job_id"]
    
    # 2. Job exists and is QUEUED
    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job is not None
        assert job.status == JobStatus.QUEUED
        
    # 3. Simulate worker execution
    with Session(engine) as session:
        job = session.get(Job, job_id)
        process_job(session,job)
        
    # 4. Job is SUCCEEDED
    with Session(engine) as session:
        job = session.get(Job, job_id)
        assert job.status == JobStatus.SUCCEEDED
        assert job.started_at is not None
        assert job.finished_at is not None
        
    # 5. Results exist
    with Session(engine) as session:
        results = session.exec(
            select(ResultRow)
            .where(ResultRow.job_id == job_id)
        ).all()
        assert len(results)>0
        for r in results:
            assert start_date <= r.business_date <= end_date
            assert r.metric_name == "toy_metric"

def test_results_blocked_until_succeeded(client):
    payload = {"start_date": "2026-03-01", "end_date": "2026-03-03", "filters": {}}
    r = client.post("/jobs", json=payload)
    job_id = r.json()["job_id"]
    # results should be blocked (QUEUED)
    r2 = client.get("/results", params={"job_id": job_id, "start_date": "2026-03-01","end_date": "2026-03-03"})
    assert r2.status_code == 409

def test_list_jobs(client):
    r = client.get("/jobs", params={"limit": 10, "offset": 0})
    assert r.status_code == 200
    data = r.json()
    assert "total" in data
    assert "items" in data