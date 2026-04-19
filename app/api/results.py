from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select
from app.core.security import require_roles
from app.db.session import get_session
from app.db.models import Job, JobStatus, ResultRow
from app.schemas.results import ResultsResponse
from app.results.factory import get_results_repository

router = APIRouter(prefix="/results",tags=["results"])

@router.get("",response_model=ResultsResponse, dependencies=[Depends(require_roles("viewer"))])
def get_results(job_id:int= Query(...,ge=1), 
                start_date:date= Query(...), 
                end_date: date= Query(...), 
                session : Session=Depends(get_session)):
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Enforce job-state rule: results only available when SUCCEEDED
    if job.status != JobStatus.SUCCEEDED:
        raise HTTPException(
            status_code=409,
            detail=f"Results are not available unless job status is SUCCEEDED. Current status={job.status}",
        )
    
    repo = get_results_repository(job.output_ref)
    
    rows = repo.load_results(job_id=job_id, start_date=start_date, end_date=end_date)
    
    return ResultsResponse(job_id=job_id,start_date=start_date, end_date=end_date, rows=rows)


