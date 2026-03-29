from fastapi.testclient import TestClient
from app.main import app

client =TestClient(app)

def test_create_job():
    payload = {"start_date" : "2026-03-01", "end_date":"2026-03-10","filters":{"country":"GR"}}
    r=client.post("/jobs", json=payload)
    assert r.status_code==202
    data = r.json()
    assert "job_id" in data
    assert data["status"] in ("","","","")