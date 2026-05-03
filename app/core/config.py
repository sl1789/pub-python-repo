import os
from dotenv import load_dotenv

load_dotenv(override=True)

DATABASE_URL=os.getenv("DATABASE_URL","sqlite:///./app.db")

# JWT signing: secret MUST be provided in any non-dev environment.
# We refuse to start with the dev sentinel or an empty value unless
# APP_ENV explicitly says "dev".
_DEV_JWT_SENTINEL = "dev-only-change-me"
APP_ENV = os.getenv("APP_ENV", "dev").lower()
JWT_SECRET = os.getenv("JWT_SECRET", _DEV_JWT_SENTINEL)
if APP_ENV != "dev" and (not JWT_SECRET or JWT_SECRET == _DEV_JWT_SENTINEL):
    raise RuntimeError(
        "JWT_SECRET must be set to a strong, non-default value when APP_ENV != 'dev'"
    )

# Algorithm is hardcoded server-side; env can only pick from an allowlist.
_ALLOWED_JWT_ALGORITHMS = {"HS256", "HS384", "HS512"}
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
if JWT_ALGORITHM not in _ALLOWED_JWT_ALGORITHMS:
    raise RuntimeError(
        f"JWT_ALGORITHM must be one of {_ALLOWED_JWT_ALGORITHMS}, got {JWT_ALGORITHM!r}"
    )

JWT_EXPIRES_MINUTES = int(os.getenv("JWT_EXPIRES_MINUTES", "60"))
DEMO_USER_USERNAME = os.getenv("DEMO_USER_USERNAME", "demo")
DEMO_USER_PASSWORD = os.getenv("DEMO_USER_PASSWORD", "demo123")
DEMO_USER_ROLES = [
    r.strip() for r in os.getenv("DEMO_USER_ROLES","viewer").split(",") 
    if r.strip()
]

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