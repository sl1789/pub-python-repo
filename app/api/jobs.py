from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select, func
from typing import Optional
from app.db.session import get_session
from app.db.models import Job, JobStatus
from app.schemas.jobs import JobCreateRequest, JobCreateResponse, JobResponse,JobListResponse

router = APIRouter(prefix="/jobs",tags=["jobs"])

def to_job_response(job: Job) -> JobResponse:
    return JobResponse(
        job_id=job.id,
        status=job.status,
        submitted_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        runner=job.runner,
        external_run_id=job.external_run_id,
        output_ref=job.output_ref,
        error_message=job.error_message,
        )

@router.post("", response_model=JobCreateResponse, status_code=202)
def create_job(req: JobCreateRequest, session: Session = Depends(get_session)):
    # validated by Pydantic validator too, but keep explicit guardrails:
    if req.start_date > req.end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    
    job = Job(
        status=JobStatus.QUEUED, 
        params={
            "start_date" : req.start_date.isoformat(),
            "end_date" : req.end_date.isoformat(),
            "filters" : req.filters,
        },
        runner="local",
        updated_at=datetime.now(),
    )
    session.add(job)
    session.commit()
    session.refresh(job)
    return JobCreateResponse(job_id=job.id, status= job.status)

@router.get("/{job_id}",response_model=JobResponse)
def get_job(job_id: int, session : Session=Depends(get_session)):
    job = session.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return to_job_response(job)

@router.get("", response_model=JobListResponse)
def list_jobs(
    status: Optional[JobStatus] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
    ):
    # count total
    count_stmt = select(func.count()).select_from(Job)
    if status:
        count_stmt = count_stmt.where(Job.status == status)
    total = session.exec(count_stmt).one()
    # fetch items
    stmt = select(Job).order_by(Job.created_at.desc()).offset(offset).limit(limit)
    if status:
        stmt = stmt.where(Job.status == status)
    jobs = session.exec(stmt).all()
    return JobListResponse(total=total, items=[to_job_response(j) for j in jobs])