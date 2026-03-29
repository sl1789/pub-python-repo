from datetime import date
from typing import Optional,Dict,Any
from pydantic import BaseModel 

class JobCreateRequest(BaseModel):
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
    
