import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session
from app.core.security import require_roles
from app.db.session import get_session
from app.db.models import Job, JobStatus
from app.schemas.results import ResultsResponse
from app.results.factory import get_results_repository

router = APIRouter(prefix="/results", tags=["results"])
logger = logging.getLogger(__name__)


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

    if not job.output_ref:
        raise HTTPException(
            status_code=409,
            detail="Job succeeded but has no output_ref; the runner did not record an export path.",
        )

    try:
        repo = get_results_repository(job.output_ref)
    except ValueError as e:
        # Misconfigured output_ref (unknown scheme, empty path, ...).
        raise HTTPException(status_code=500, detail=f"Bad output_ref: {e}") from e
    except RuntimeError as e:
        # Missing storage credentials.
        raise HTTPException(status_code=503, detail=str(e)) from e

    try:
        rows = repo.load_results(job.params or {})
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"No parquet found at {job.output_ref}: {e}",
        ) from e
    except PermissionError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Storage auth failed: {e}",
        ) from e
    except Exception as e:
        # Log the full traceback server-side; surface the message to the caller
        # so the UI can show something more useful than a bare 500.
        logger.exception("load_results failed for job_id=%s", job_id)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load results: {type(e).__name__}: {e}",
        ) from e

    return ResultsResponse(job_id=job_id, rows=rows)

