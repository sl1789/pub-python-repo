from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _auth_headers():
    r = client.post("/auth/token", data={"username": "demo", "password": "demo123"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@patch.dict(
    "os.environ",
    {
        "DATABRICKS_HOST": "https://example.com",
        "DATABRICKS_TOKEN": "x",
        "DATABRICKS_MC_JOB_ID": "1",
        "AZURE_STORAGE_ACCOUNT": "acct",
        "AZURE_STORAGE_KEY": "key",
    },
    clear=False,
)
def test_create_mc_job():
    payload = {
        "ticker": "AAPL",
        "strike": 150.0,
        "period_days": 10,
        "num_simulations": 1000,
    }
    r = client.post("/jobs", json=payload, headers=_auth_headers())
    assert r.status_code == 202, r.text
    data = r.json()
    assert "job_id" in data
    assert data["status"] == "QUEUED"


def test_create_job_rejects_bad_ticker():
    payload = {
        "ticker": "BAD TICKER!",
        "strike": 150.0,
        "period_days": 10,
        "num_simulations": 1000,
    }
    r = client.post("/jobs", json=payload, headers=_auth_headers())
    assert r.status_code == 422


def test_create_job_rejects_non_databricks_runner():
    payload = {
        "runner": "local",
        "ticker": "AAPL",
        "strike": 150.0,
        "period_days": 10,
        "num_simulations": 1000,
    }
    r = client.post("/jobs", json=payload, headers=_auth_headers())
    assert r.status_code == 422
