from typing import Any, Dict, List
from pydantic import BaseModel


class ResultsResponse(BaseModel):
    job_id: int
    rows: List[Dict[str, Any]]
