import re
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, model_validator
from app.db.models import JobStatus

# Allowlist for ticker symbols, mirrored from the Databricks helpers.
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-^=]{1,16}$")


class JobCreateRequest(BaseModel):
    """
    Parameters for a Monte Carlo simulation job. The simulation is executed
    by the `monte_carlo_simulation` Databricks notebook, so `runner` must be
    "databricks".
    """
    runner: str = "databricks"
    ticker: str
    strike: float
    period_days: int
    num_simulations: int

    @model_validator(mode="after")
    def validate_params(self):
        if self.runner != "databricks":
            raise ValueError("monte_carlo jobs require runner='databricks'")
        if not self.ticker or not _TICKER_RE.match(self.ticker):
            raise ValueError("invalid ticker")
        if self.strike <= 0:
            raise ValueError("strike must be > 0")
        if self.period_days <= 0:
            raise ValueError("period_days must be > 0")
        if self.num_simulations <= 0:
            raise ValueError("num_simulations must be > 0")
        return self


class JobCreateResponse(BaseModel):
    job_id: int
    status: JobStatus


class JobResponse(BaseModel):
    job_id: int
    status: JobStatus
    submitted_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    runner: str
    external_run_id: Optional[str] = None
    output_ref: Optional[str] = None
    error_message: Optional[str] = None


class ResultsResponse(BaseModel):
    job_id: int
    rows: List[Dict[str, Any]]


class JobListResponse(BaseModel):
    total: int
    items: List[JobResponse]
