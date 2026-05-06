from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session
from app.core.security import require_roles
from app.db.session import get_session
from app.db.models import Job, JobStatus
from app.schemas.results import ResultsResponse
from app.results.factory import get_results_repository

router = APIRouter(prefix="/results", tags=["results"])


@router.get("", response_model=ResultsResponse, dependencies=[Depends(require_roles("viewer"))])
def get_results(
    job_id: int = Query(..., ge=1),
    session: Session = Depends(get_session),
):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.SUCCEEDED:
        raise HTTPException(
            status_code=409,
            detail=f"Results are not available unless job status is SUCCEEDED. Current status={job.status}",
        )

    repo = get_results_repository(job.output_ref)
    rows = repo.load_results(job.params or {})
    return ResultsResponse(job_id=job_id, rows=rows)

