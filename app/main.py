import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
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
from app.core.security_middleware import (
    RateLimitMiddleware,
    SecurityHeadersMiddleware,
    EnforceJsonContentTypeMiddleware,
)

print("PYTHONPATH:", sys.path)

#setup_logging(service_name="api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Alembic owns schema in normal deployments. Auto-create only when
    # explicitly requested (e.g. local SQLite dev).
    if os.getenv("APP_AUTO_CREATE_TABLES", "false").lower() in ("1", "true", "yes"):
        init_db_and_seed()
    yield


app = FastAPI(title="Job Orchestrator API", lifespan=lifespan)

# Middleware
app.add_middleware(RequestIdMiddleware)

# security middleware
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(EnforceJsonContentTypeMiddleware)
app.add_middleware(RateLimitMiddleware, max_requests=120, window_seconds=60)

# Exception handlers
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)

app.include_router(health_router)
app.include_router(auth_router)
app.include_router(job_router)
app.include_router(results_router)

