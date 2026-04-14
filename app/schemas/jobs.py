from datetime import date
from typing import Optional,Dict,Any, List
from pydantic import BaseModel , Field
from app.db.models import Job, JobStatus, ResultRow

class JobCreateRequest(BaseModel):
    """
    Parameters coming from the UI.
    Keep it explicit so you can validate strongly.
    """
    start_date:date
    end_date:date
    filters:Dict[str,Any]={}
    
class JobCreateResponse(BaseModel):
    job_id:int
    status:str
    
class JobResponse(BaseModel):
    job_id:int
    status:str
    output_ref:Optional[str] = None
    error_message:Optional[str] = None
    
class ResultsResponse(BaseModel):
    start_date: date
    end_date: date
    rows: List[Dict[str, Any]]