from __future__ import annotations
import time
from collections import defaultdict, deque
from typing import Deque, Dict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple in-memory fixed-window limiter.
    Good for dev; in production use gateway/Redis.
    """
    
    def __init__(self, app, max_requests: int = 120, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.hits: Dict[str, Deque[float]] = defaultdict(deque)
        
    async def dispatch(self, request: Request, call_next):
        # Identify client by auth subject if present, else IP
        client_key = request.headers.get("Authorization") or (request.client.host if request.client else "unknown")
        now = time.time()
        q = self.hits[client_key]
        
        # Remove old timestamps
        while q and now - q[0] > self.window_seconds:
            q.popleft()
            
        if len(q) >= self.max_requests:
            return JSONResponse(
                status_code=429,
                content={"error": "rate_limited", "detail": "Too many requests"}
            )

        q.append(now)
        return await call_next(request)
    
    
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        resp = await call_next(request)
        # Basic security headers
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["Cache-Control"] = "no-store"
        return resp
    
class EnforceJsonContentTypeMiddleware(BaseHTTPMiddleware):
    """
    Enforce Content-Type header for requests with bodies.
    Policy calls out secure Content-Type configuration.
    """
    
    async def dispatch(self, request: Request, call_next):
        if request.method in ("POST", "PUT", "PATCH"):
            content_type = request.headers.get("content-type", "")
            
            #Allow OAuth2 token exchange (form-encoded)
            if request.url.path == "/auth/token":
                if "application/x-www-form-urlencoded" not in content_type:
                    return JSONResponse(
                        status_code=415,
                        content={"error": "unsupported_media_type", 
                                 "detail": "Use application/x-www-form-urlencoded for /auth/token"}
                    )
                return await call_next(request)
            
            
            if "application/json" not in content_type and "multipart/form-data" not in content_type:
                return JSONResponse(
                    status_code=415,
                    content={"error": "unsupported_media_type", "detail": "Use application/json"}
                )
        return await call_next(request)