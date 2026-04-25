import time
from datetime import date, timedelta
import pytest

from fastapi.testclient import TestClient
from sqlmodel import Session, select

from app.main import app

@pytest.fixture
def client(): 
    return TestClient(app)

def test_results_and_metrics(client):
    job_id = 1
    results = client.get(
    f"/results?job_id={job_id}&start_date=2024-01-01&end_date=2024-01-03"
    )
    assert results.status_code == 200
    metrics = client.get(f"/jobs/{job_id}/metrics")
    assert metrics.status_code == 200
    assert metrics.json()["row_count"] > 0