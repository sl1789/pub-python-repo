from fastapi import FastAPI, Depends, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.core.middleware import RequestIdMiddleware
from app.core.errors import http_exception_handler, validation_exception_handler
from app.db.init_db import init_db_and_seed
from app.api.health import router as health_router
from app.api.jobs import router as job_router
from app.api.results import router as results_router
from app.api.auth import router as auth_router
from app.core.logging_config import setup_logging
from app.core.security_middleware import RateLimitMiddleware,SecurityHeadersMiddleware, EnforceJsonContentTypeMiddleware
import sys
#from __future__ import annotations

print("PYTHONPATH:",sys.path)

#setup_logging(service_name="api")

app = FastAPI(title="Job Orchestrator API")

# Middleware
app.add_middleware(RequestIdMiddleware)

# security middleware
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(EnforceJsonContentTypeMiddleware)
app.add_middleware(RateLimitMiddleware, max_requests=120, window_seconds=60)

# Exception handlers
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)

@app.on_event("startup")
def startup():
    # If Alembic owns schema completely, you can remove this call.
    init_db_and_seed()
    
app.include_router(health_router)
app.include_router(auth_router)
app.include_router(job_router)
app.include_router(results_router)
