from sqlmodel import create_engine, Session
from app.core.config import DATABASE_URL

# SQLite needs check_same_thread=False because FastAPI/Starlette run sync
# endpoints in a thread pool. No-op for other backends.
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, echo=False, connect_args=_connect_args)

def get_session():
    with Session(engine) as session:
        yield session