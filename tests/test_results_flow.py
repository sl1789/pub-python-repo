from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_headers():
    r = client.post("/auth/token", data={"username": "demo", "password": "demo123"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def test_results_blocked_until_succeeded():
    # Submit a Monte Carlo job; it will sit in QUEUED until the worker picks it up.
    payload = {
        "ticker": "AAPL",
        "strike": 150.0,
        "period_days": 10,
        "num_simulations": 1000,
    }
    headers = _auth_headers()
    r = client.post("/jobs", json=payload, headers=headers)
    assert r.status_code == 202, r.text
    job_id = r.json()["job_id"]

    # Results should be blocked because the job is not yet SUCCEEDED.
    r2 = client.get("/results", params={"job_id": job_id}, headers=headers)
    assert r2.status_code == 409
