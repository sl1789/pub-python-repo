from fastapi import FastAPI, Depends, HTTPException
from app.db.init_db import init_db_and_seed
from app.api.health import router as health_router
from app.api.jobs import router as job_router
from app.api.results import router as results_router
import sys
#from future import annotations

print("PYTHONPATH:",sys.path)

app = FastAPI(title="Toy Job Orchestrator")

@app.on_event("startup")
def startup():
    init_db_and_seed()
    
app.include_router(health_router)
app.include_router(job_router)
app.include_router(results_router)
