from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional
from app.db.models import JobStatus

class RunnerError(RuntimeError):
    pass

@dataclass(frozen=True)
class SubmitResult:
    external_run_id: Optional[str] = None
    output_ref: Optional[str] = None
    
@dataclass(frozen=True)
class PollResult:
    status: JobStatus
    output_ref: Optional[str] = None
    error_message: Optional[str] = None
    
class BaseRunner(ABC):
    """
    A Runner is responsible for:
    - submitting a job to some execution environment
    - (optionally) polling that environment for status
    - (optionally) cancelling the run
    The worker orchestrates state transitions in our metadata DB.
    """
    
    @abstractmethod
    def submit(self, job_id: int, params: Dict[str, Any]) -> SubmitResult:
        ...
    
    @abstractmethod
    def poll(self, external_run_id: Optional[str]) -> Optional[PollResult]:
        """
        Return PollResult if status is known, else None.
        """
        ...
    
    def cancel(self, external_run_id: Optional[str]) -> None:
        # optional; not all backends support cancel
        return