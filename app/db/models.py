from datetime import datetime, date
from enum import Enum
from typing import Optional,Dict,Any
#from __future__ import annotations
from sqlmodel import SQLModel, Field 
from sqlalchemy import Column
from sqlalchemy.types import JSON

class JobStatus(str, Enum):
    QUEUED="QUEUED"
    RUNNING="RUNNING"
    SUCCEEDED="SUCCEEDED"
    FAILED="FAILED"
    
class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now, index=True)
    updated_at: datetime = Field(default_factory=datetime.now, index=True)
    status: JobStatus = Field(default=JobStatus.QUEUED, index=True)
    
    # store UI parameters as JSON
    params: Dict[str,Any] = Field(default_factory=dict, sa_column=Column(JSON))
    
    # execution metadata (future Spark integration)
    runner: str = Field(default="local", index=True)
    external_run_id: Optional[str] = Field(default=None, index=True)
    
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    
    output_ref : Optional[str] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    
    #attempts: int = 0
    #max_attempts: int = 3
    
class ResultRow(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # NEW: connect results to a job run
    job_id: int = Field(index=True)
    
    business_date: date = Field(index=True)
    metric_name: str = Field(index=True)
    metric_value : float
    
class JobEvent(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(index=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    event_type: str
    message: Optional[str] = None
    
    
class JobMetrics(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(index=True)
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    row_count: int
    runner: str
    status: str
