from datetime import date,datetime
from typing import Optional,Dict,Any, List
from pydantic import BaseModel , Field, model_validator
from app.db.models import Job, JobStatus, ResultRow

class JobCreateRequest(BaseModel):
    """
    Parameters coming from the UI.
    Keep it explicit so you can validate strongly.
    """
    start_date:date
    end_date:date
    filters:Dict[str,Any]=Field(default_factory=dict)
    
    @model_validator(mode="after")
    def validate_dates(self):
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self
    
class JobCreateResponse(BaseModel):
    job_id:int
    status:JobStatus
    
class JobResponse(BaseModel):
    job_id:int
    status:JobStatus
    submitted_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    runner: str
    external_run_id: Optional[str] = None
    output_ref: Optional[str] = None
    error_message: Optional[str] = None
    
class ResultsResponse(BaseModel):
    job_id:int
    start_date: date
    end_date: date
    rows: List[Dict[str, Any]]
    
class JobListResponse(BaseModel):
    total: int
    items: List[JobResponse]