from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session

from app.db.session import get_session
from app.db.models import Job, JobStatus
from app.schemas.jobs import JobCreateRequest, JobCreateResponse, JobResponse

router = APIRouter(prefix="/jobs",tags=["jobs"])

@router.post("", response_model=JobCreateResponse, status_code=202)
def create_job(req: JobCreateRequest, session: Session = Depends(get_session)):
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
    
    return JobResponse(
        job_id=job.id,
        status=job.status,
        output_ref=job.output_ref,
        error_message=job.error_message
        )