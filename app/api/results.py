from datetime import date
from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from app.db.session import get_session
from app.db.models import ResultRow
from app.schemas.results import ResultsResponse

router = APIRouter(prefix="/results",tags=["results"])

@router.get("",response_model=ResultsResponse)
def get_results(job_id:int, start_date:date, end_date: date, session : Session=Depends(get_session)):
    stmt = (
        select(ResultRow)
        .where(ResultRow.job_id==job_id)
        .where(ResultRow.business_date>=start_date, ResultRow.business_date<=end_date))
    rows=session.exec(stmt).all()
    payload=[
        {
            "business_date":r.business_date.isoformat(),
            "metric_name":r.metric_name,
            "metric_value":r.metric_value
            } 
        for r in rows
        ]
    
    return ResultsResponse(start_date=start_date, end_date=end_date, rows=payload)


