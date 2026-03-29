from datetime import datetime, date
from enum import Enum
from typing import Optional,Dict,Any

from sqlmodel import SQLModel, Field 
from sqlalchemy import Column
from sqlalchemy.types import JSON

class JobStatus(str, Enum):
    QUEUED="QUEUED"
    RUNNING="RUNNING"
    SUCCEDEED="SUCCEDEED"
    FAILED="FAILED"
    
class Job(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    status: JobStatus = Field(default=JobStatus.QUEUED, index=True)
    
    # store UI parameters as JSON
    params: Dict[str,Any] = Field(default_factory=dict, sa_column=Column(JSON))
    
    output_ref : Optional[str] = Field(default=None)
    error_message: Optional[str] = Field(default=None)
    
class ResultRow(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    business_date: date = Field(index=True)
    metric_name: str = Field(index=True)
    metric_value : float
    
