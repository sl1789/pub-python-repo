from fastapi import FastAPI, Depends, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.core.middleware import RequestIdMiddleware
from app.core.errors import http_exception_handler, validation_exception_handler
from app.db.init_db import init_db_and_seed
from app.api.health import router as health_router
from app.api.jobs import router as job_router
from app.api.results import router as results_router
import sys
#from future import annotations

print("PYTHONPATH:",sys.path)

app = FastAPI(title="Job Orchestrator API")

# Middleware
app.add_middleware(RequestIdMiddleware)
# Exception handlers
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)

@app.on_event("startup")
def startup():
    # If Alembic owns schema completely, you can remove this call.
    init_db_and_seed()
    
app.include_router(health_router)
app.include_router(job_router)
app.include_router(results_router)
