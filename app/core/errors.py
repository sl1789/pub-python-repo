from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette import status
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.schemas.errors import ErrorResponse

def request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)

async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    payload = ErrorResponse(
    error="http_error",
    detail={"status_code": exc.status_code, "message": exc.detail},
    request_id=request_id(request),
        )
    return JSONResponse(status_code=exc.status_code,content=payload.model_dump())

async def validation_exception_handler(request: Request, exc:RequestValidationError):
    payload = ErrorResponse(
    error="validation_error",
    detail=exc.errors(),
    request_id=request_id(request),
        )
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,content=payload.model_dump())