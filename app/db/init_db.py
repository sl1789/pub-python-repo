# create tables and seed sample results

from datetime import date, timedelta
from sqlmodel import SQLModel, Session, select

from app.db.session import engine
from app.db.models import ResultRow

def init_db_and_seed():
    SQLModel.metadata.create_all(engine)
    
    # Seed toy results if empty
    # with Session(engine) as session:
    #     existing = session.exec(select(ResultRow).limit(1)).first()
    #     if existing:
    #         return
        
        # start = date.today() - timedelta(days=14)
        # rows = []
        # for i in range(15):
        #     d=start + timedelta(days=i)
        #     rows.append(ResultRow(business_date=d, metric_name="toy_metric", metric_value=100+i))
        #     session.add_all(rows)
        #     session.commit()