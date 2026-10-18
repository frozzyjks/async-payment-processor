import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.schemas import ErrorBody, ErrorResponse
from app.domain.errors import IdempotencyConflictError, PaymentNotFoundError

logger = logging.getLogger(__name__)

RETRY_HINT = "retry the request with the same Idempotency-Key"

_HTTP_ERROR_CODES = {
    status.HTTP_401_UNAUTHORIZED: "unauthorized",
    status.HTTP_403_FORBIDDEN: "forbidden",
    status.HTTP_404_NOT_FOUND: "not_found",
    status.HTTP_405_METHOD_NOT_ALLOWED: "method_not_allowed",
}


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def error_response(
    status_code: int,
    code: str,
    message: str,
    details: Any | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(error=ErrorBody(code=code, message=message, details=details))
    return JSONResponse(
        status_code=status_code, content=jsonable_encoder(body.model_dump()), headers=headers
    )


def _service_unavailable(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Dependency unavailable", extra={"path": request.url.path}, exc_info=exc)
    return error_response(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        "service_unavailable",
        f"Service is temporarily unavailable, {RETRY_HINT}",
    )


def _internal_error(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Unhandled error", extra={"path": request.url.path}, exc_info=exc)
    return error_response(
        status.HTTP_500_INTERNAL_SERVER_ERROR, "internal_error", "Internal server error"
    )


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(_: Request, exc: ApiError) -> JSONResponse:
        return error_response(exc.status_code, exc.code, exc.message)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _HTTP_ERROR_CODES.get(exc.status_code, "http_error")
        headers = dict(exc.headers) if exc.headers else None
        return error_response(exc.status_code, code, str(exc.detail), headers=headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {"loc": list(error["loc"]), "message": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return error_response(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "validation_error",
            "Request validation failed",
            details,
        )

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict(_: Request, exc: IdempotencyConflictError) -> JSONResponse:
        return error_response(status.HTTP_409_CONFLICT, "idempotency_conflict", str(exc))

    @app.exception_handler(PaymentNotFoundError)
    async def payment_not_found(_: Request, exc: PaymentNotFoundError) -> JSONResponse:
        return error_response(status.HTTP_404_NOT_FOUND, "payment_not_found", "Payment not found")

    @app.exception_handler(DBAPIError)
    async def database_error(request: Request, exc: DBAPIError) -> JSONResponse:
        if exc.connection_invalidated or isinstance(exc, OperationalError | InterfaceError):
            return _service_unavailable(request, exc)
        return _internal_error(request, exc)

    @app.exception_handler(OSError)
    async def connection_refused(request: Request, exc: OSError) -> JSONResponse:
        return _service_unavailable(request, exc)

    @app.exception_handler(Exception)
    async def unhandled_error(request: Request, exc: Exception) -> JSONResponse:
        return _internal_error(request, exc)
