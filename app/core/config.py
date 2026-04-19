import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL","sqlite:///./app.db")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-only-change-me")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256") # force server-side
JWT_EXPIRES_MINUTES = int(os.getenv("JWT_EXPIRES_MINUTES", "60"))
DEMO_USER_USERNAME = os.getenv("DEMO_USER_USERNAME", "demo")
DEMO_USER_PASSWORD = os.getenv("DEMO_USER_PASSWORD", "demo123")
DEMO_USER_ROLES = [r.strip() for r in os.getenv("DEMO_USER_ROLES","viewer").split(",") if r.strip()]

API_HOST=os.getenv("API_HOST","127.0.0.1")
API_PORT=int(os.getenv("API_PORT","8000"))

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
AZURE_RESULTS_CONTAINER = os.getenv("AZURE_RESULTS_CONTAINER", "results")
AZURE_RESULTS_PREFIX = os.getenv("AZURE_RESULTS_PREFIX", "export")
# For toy use: storage account key (do not commit)
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY", "")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "")