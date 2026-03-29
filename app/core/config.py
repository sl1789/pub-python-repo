import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL","sqlite:///./app.db")
API_HOST=os.getenv("API_HOST","127.0.0.1")
API_PORT=int(os.getenv("API_PORT","8000"))