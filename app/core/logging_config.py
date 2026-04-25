import logging
import sys
from typing import Optional

def setup_logging(
    level:int = logging.INFO,
    service_name: str = "job-orchestrator"
):
    """
    Configure Application-wide structured logging.
    Safe to call once per process.
    """
    
    root= logging.getLogger()
    root.saveLevel(level)
    
    # Avoid duplicate handlers if reloaded (important for FastAPI)
    if root.handlers:
        return
    
    handler = logging.StreamHandler(sys.stdout)
    
    formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | %(levelname)s | %(name)s | service=%(service)s | %(message)s | %(job_id)s | %(event)s"
        )
    )
    
    handler.setFormatter(formatter)
    root.addHandler(handler)
    
    # Inject service name into all log records
    logging.LoggerAdapter(logging.getLogger(),{"service":service_name})