from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel

from app.core.config import (
JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRES_MINUTES,
DEMO_USER_USERNAME, DEMO_USER_PASSWORD, DEMO_USER_ROLES
)


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    
class User(BaseModel):
    username: str
    roles: List[str]
    
# Demo user store (replace with DB later)
_demo_password_hash = pwd_context.hash(DEMO_USER_PASSWORD)
_DEMO_USER = {
    "username": DEMO_USER_USERNAME,
    "password_hash": _demo_password_hash,
    "roles": DEMO_USER_ROLES,
    }

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def authenticate_user(username: str, password: str) -> Optional[User]:
    # NOTE: demo only; replace with DB lookup
    if username != _DEMO_USER["username"]:
        return None
    if not verify_password(password, _DEMO_USER["password_hash"]):
        return None
    return User(username=username, roles=_DEMO_USER["roles"])

def create_access_token(user: User) -> str:
    """
    JWT payload MUST avoid sensitive data (policy).
    Keep it minimal: subject + roles + exp.
    """
    
    now = datetime.now()
    exp = now + timedelta(minutes=JWT_EXPIRES_MINUTES)
    payload = {
        "sub": user.username,
        "roles": user.roles,
        # subject
        # authorization claims
        "exp": exp,
        "iat": now,
        }
    
    # IMPORTANT: algorithm is forced server-side; not read from token header/payload.
    
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    try:
        decoded = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username = decoded.get("sub")
        roles = decoded.get("roles") or []
        if not username:
            raise HTTPException(status_code=401, detail="Invalid token (missing sub)")
        return User(username=username, roles=list(roles))
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
def require_roles(*required: str):
    """
    RBAC dependency.Example: Depends(require_roles("submitter"))
    """
    def _dep(user: User = Depends(get_current_user)) -> User:
        if not set(required).issubset(set(user.roles)):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required roles: {required}, current roles: {user.roles} , input: {DEMO_USER_ROLES}"
                )
        return user
    return _dep