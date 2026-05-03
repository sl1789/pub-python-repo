from __future__ import annotations
import hashlib
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
    
    # Hard cap on tracked clients to prevent unbounded memory growth (DoS).
    _MAX_TRACKED_CLIENTS = 10_000
    
    def __init__(self, app, max_requests: int = 120, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.hits: Dict[str, Deque[float]] = defaultdict(deque)
    
    @staticmethod
    def _client_key(request: Request) -> str:
        # Hash the Authorization header so raw bearer tokens never sit in
        # process memory as dict keys. Fall back to remote IP.
        auth = request.headers.get("Authorization")
        if auth:
            return "a:" + hashlib.sha256(auth.encode("utf-8")).hexdigest()
        return "ip:" + (request.client.host if request.client else "unknown")
        
    def _evict_stale(self, now: float) -> None:
        # Drop entries whose window is fully expired so the dict doesn't grow
        # unbounded over time.
        stale = [k for k, q in self.hits.items() if not q or now - q[-1] > self.window_seconds]
        for k in stale:
            self.hits.pop(k, None)
        # Hard cap as a final safety net.
        if len(self.hits) > self._MAX_TRACKED_CLIENTS:
            # Drop oldest-touched half.
            for k in sorted(self.hits, key=lambda k: self.hits[k][-1] if self.hits[k] else 0)[: len(self.hits) // 2]:
                self.hits.pop(k, None)
        
    async def dispatch(self, request: Request, call_next):
        client_key = self._client_key(request)
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
        # Periodic eviction sweep, cheap amortized.
        if len(self.hits) > self._MAX_TRACKED_CLIENTS // 2:
            self._evict_stale(now)
        return await call_next(request)
    
    
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        resp = await call_next(request)
        # Basic security headers
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["Cache-Control"] = "no-store"
        # Assume HTTPS in production; harmless over HTTP for browsers.
        resp.headers.setdefault(
            "Strict-Transport-Security", "max-age=31536000; includeSubDomains"
        )
        # Lock down browser features and inline content.
        resp.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; frame-ancestors 'none'",
        )
        resp.headers.setdefault(
            "Permissions-Policy", "geolocation=(), microphone=(), camera=()"
        )
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
            
            # Only JSON bodies are accepted on the rest of the API. The previous
            # multipart/form-data allowance was unused and broadened the parser
            # attack surface unnecessarily.
            if "application/json" not in content_type:
                return JSONResponse(
                    status_code=415,
                    content={"error": "unsupported_media_type", "detail": "Use application/json"}
                )
        return await call_next(request)