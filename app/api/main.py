from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.errors import register_error_handlers
from app.api.routes import router
from app.api.security import install_api_key_auth
from app.config import Settings, get_settings
from app.db.session import SessionFactory, create_engine, create_session_factory
from app.logging_config import configure_logging
from app.services.payments import PaymentService


def create_app(settings: Settings, session_factory: SessionFactory | None = None) -> FastAPI:

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if session_factory is not None:
            yield
            return
        engine = create_engine(settings)
        _attach_services(app, create_session_factory(engine))
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Async Payment Processor",
        version="1.0.0",
        description="Every endpoint requires the `X-API-Key` header.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    if session_factory is not None:
        _attach_services(app, session_factory)
    register_error_handlers(app)
    install_api_key_auth(app, settings)
    app.include_router(router)
    return app


def _attach_services(app: FastAPI, session_factory: SessionFactory) -> None:
    app.state.session_factory = session_factory
    app.state.payment_service = PaymentService(session_factory)


def build_default_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    return create_app(settings)
