import logging
import os
import sys
import time
from datetime import datetime, timezone

# Adds the repo root to sys.path so `app.*` imports resolve when run directly.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sqlmodel import Session, select
from app.db.session import engine
from app.db.models import Job, JobStatus

from app.runners.factory import get_runner
from app.runners.base import RunnerError


logger = logging.getLogger("worker")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


POLL_SECONDS = 1.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def mark_running(
    session: Session,
    job: Job,
    external_run_id: str | None = None,
    external_output_ref: str | None = None,
):
    job.status = JobStatus.RUNNING
    job.started_at = job.started_at or _utcnow()
    job.updated_at = _utcnow()
    if external_run_id:
        job.external_run_id = external_run_id
    if external_output_ref is not None:
        job.output_ref = external_output_ref
    session.add(job)
    session.commit()
    session.refresh(job)


def apply_poll_result(
    session: Session,
    job: Job,
    status: JobStatus,
    output_ref: str | None,
    error_message: str | None,
):
    job.status = status
    job.updated_at = _utcnow()

    if status == JobStatus.SUCCEEDED:
        job.finished_at = _utcnow()
        # Only overwrite output_ref if the runner gave us a fresh one;
        # otherwise keep whatever was stored at submit time.
        if output_ref is not None:
            job.output_ref = output_ref
        job.error_message = None

    if status == JobStatus.FAILED:
        job.finished_at = _utcnow()
        job.output_ref = None
        job.error_message = error_message or "Unknown failure"

    session.add(job)
    session.commit()


def process_queued_job(session: Session, job: Job):
    runner = get_runner(job.runner)
    submit = runner.submit(job_id=job.id, params=job.params)
    mark_running(
        session,
        job,
        external_run_id=submit.external_run_id,
        external_output_ref=submit.output_ref,
    )


def poll_running_jobs(session: Session):
    running_jobs = session.exec(select(Job).where(Job.status == JobStatus.RUNNING)).all()
    for job in running_jobs:
        try:
            runner = get_runner(job.runner)
            poll = runner.poll(job.external_run_id)
            if poll:
                apply_poll_result(session, job, poll.status, poll.output_ref, poll.error_message)
        except RunnerError as e:
            # Treat runner poll errors as transient; keep job RUNNING but record last error
            job.updated_at = _utcnow()
            job.error_message = f"Poll error: {e}"
            session.add(job)
            session.commit()


def main():
    logger.info("Worker started (Runner-based). Polling for jobs...")
    while True:
        with Session(engine) as session:
            # 1) Poll external running jobs (must not crash the loop)
            try:
                poll_running_jobs(session)
            except Exception:
                logger.exception("poll_running_jobs failed; continuing")

            # 2) Pick up one queued job (FIFO)
            try:
                job = session.exec(
                    select(Job)
                    .where(Job.status == JobStatus.QUEUED)
                    .order_by(Job.created_at)
                    .limit(1)
                ).first()
            except Exception:
                logger.exception("failed to query queued jobs; sleeping")
                time.sleep(POLL_SECONDS)
                continue

            if job:
                try:
                    logger.info("Processing queued job %s runner=%s", job.id, job.runner)
                    process_queued_job(session, job)
                except Exception:
                    # Log full traceback server-side; persist a generic message
                    # so the API never leaks internal details to callers.
                    logger.exception("Job %s failed during processing", job.id)
                    apply_poll_result(
                        session, job, JobStatus.FAILED, None, "job failed; see server logs"
                    )
            else:
                time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
