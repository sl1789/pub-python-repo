import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.db import init_db

@pytest.fixture(scope="session", autouse=True)
def setup_db():
    init_db()
    yield

@pytest.fixture
def client():
    return TestClient(app)