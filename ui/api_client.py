import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE= os.getenv("API_BASE","http://127.0.0.1:8000")

def health():
    return requests.get(f"{API_BASE}/health", timeout=10).json()

def submit_job(start_date, end_date, filters):
    payload={
        "start_date":str(start_date),
        "end_date":str(end_date),
        "filters":filters
    }
    return requests.post(f"{API_BASE}/jobs",json=payload, timeout=30).json()

def get_jobs(job_id:int):
    return requests.get(f"{API_BASE}/jobs/{job_id}", timeout=10).json()

def get_results(start_date, end_date):
    params={
       "start_date":str(start_date),
        "end_date":str(end_date) 
    }
    return requests.get(f"{API_BASE}/results",params=params, timeout=30).json()