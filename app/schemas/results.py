from datetime import date
from typing import Optional,Dict,Any, List
from pydantic import BaseModel 

class ResultsResponse(BaseModel):
    start_date:date
    end_date:date
    rows:List[Dict[str,Any]]