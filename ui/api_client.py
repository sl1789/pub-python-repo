import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE= os.getenv("API_BASE","http://127.0.0.1:8000")

def login(username: str, password: str) -> str:
    r = requests.post(
        f"{API_BASE}/auth/token",
        data={"username": username, "password": password},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]
def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def health():
    r= requests.get(f"{API_BASE}/health", timeout=10)
    r.raise_for_status()
    return r.json()

def submit_job(token: str,start_date, end_date, filters:dict, runner: str):
    payload={
        "start_date":start_date.isoformat(),
        "end_date":end_date.isoformat(),
        "filters":filters or {},
        "runner": runner,
    }
    r=requests.post(f"{API_BASE}/jobs",json=payload, headers=_headers(token), timeout=30)
    if r.status_code>=400:
        raise RuntimeError(f"POST /jobs failed ({r.status_code}):{r.text}")
    return r.json()

def get_job(token: str,job_id:int):
    r=requests.get(f"{API_BASE}/jobs/{job_id}", headers=_headers(token), timeout=20)
    r.raise_for_status()
    return r.json()

def get_results(token: str,job_id:int ,start_date, end_date):
    params={
        "job_id":job_id,
        "start_date":start_date.isoformat(),
        "end_date":end_date.isoformat() 
    }
    r=requests.get(f"{API_BASE}/results",params=params, headers=_headers(token), timeout=30)
    r.raise_for_status()
    return r.json()