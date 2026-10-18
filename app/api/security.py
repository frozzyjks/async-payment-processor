import secrets
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.security import APIKeyHeader

from app.api.errors import error_response
from app.config import Settings

API_KEY_HEADER = "X-API-Key"
DOCS_PATHS = frozenset({"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"})

api_key_scheme = APIKeyHeader(
    name=API_KEY_HEADER,
    auto_error=False,
    description="Static API key from the API_KEY environment variable",
)


def _authentication_error(provided: str | None, expected: str) -> Response | None:
    if not provided:
        return error_response(
            status.HTTP_401_UNAUTHORIZED, "unauthorized", f"Missing {API_KEY_HEADER} header"
        )
    if not secrets.compare_digest(provided.encode(), expected.encode()):
        return error_response(status.HTTP_403_FORBIDDEN, "forbidden", "Invalid API key")
    return None


def install_api_key_auth(app: FastAPI, settings: Settings) -> None:
    expected = settings.api_key.get_secret_value()

    @app.middleware("http")
    async def authenticate(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.path not in DOCS_PATHS:
            error = _authentication_error(request.headers.get(API_KEY_HEADER), expected)
            if error is not None:
                return error
        return await call_next(request)
