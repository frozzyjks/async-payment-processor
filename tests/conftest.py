import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.api.main import create_app
from app.config import Settings
from app.db.models import Payment
from app.db.session import SessionFactory, create_session_factory
from app.domain.enums import Currency
from app.services.payments import CreatePaymentCommand, PaymentService

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_DB_NAME = os.environ.get("TEST_POSTGRES_DB", "payments_test")
TEST_API_KEY = "test-api-key-0123456789abcdef"


@pytest.fixture(scope="session")
def settings() -> Settings:
    base = Settings()
    return base.model_copy(
        update={
            "postgres_db": TEST_DB_NAME,
            "api_key": SecretStr(TEST_API_KEY),
            "rabbitmq_vhost": "payments-test",
        }
    )


async def _recreate_database(settings: Settings) -> None:
    connection = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        database="postgres",
    )
    try:
        await connection.execute(f'DROP DATABASE IF EXISTS "{TEST_DB_NAME}" WITH (FORCE)')
        await connection.execute(f'CREATE DATABASE "{TEST_DB_NAME}"')
    finally:
        await connection.close()


@pytest.fixture(scope="session")
def migrated_database(settings: Settings) -> None:
    try:
        asyncio.run(_recreate_database(settings))
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.exit(f"PostgreSQL for tests is not available: {exc!r}", returncode=2)
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.attributes["database_url"] = settings.database_url
    config.attributes["configure_logger"] = False
    command.upgrade(config, "head")


@pytest.fixture
async def session_factory(
    settings: Settings, migrated_database: None
) -> AsyncIterator[SessionFactory]:
    engine = create_async_engine(settings.database_url, poolclass=NullPool)
    try:
        yield create_session_factory(engine)
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                text("TRUNCATE payments, outbox, webhook_deliveries RESTART IDENTITY CASCADE")
            )
        await engine.dispose()


@pytest.fixture
async def client(
    settings: Settings, session_factory: SessionFactory
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings, session_factory=session_factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http_client:
        yield http_client


def payment_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1, "tags": ["a", "b"]},
        "webhook_url": "https://merchant.example.com/webhooks",
    }
    body.update(overrides)
    return body


CreatePayment = Callable[..., Awaitable[Payment]]


@pytest.fixture
def create_payment(session_factory: SessionFactory) -> CreatePayment:
    service = PaymentService(session_factory)
    counter = 0

    async def _create(**overrides: Any) -> Payment:
        nonlocal counter
        counter += 1
        values: dict[str, Any] = {
            "idempotency_key": f"key-{counter}",
            "amount": Decimal("250.00"),
            "currency": Currency.USD,
            "description": "Test payment",
            "metadata": {"n": counter},
            "webhook_url": "https://merchant.example.com/hook",
        }
        values.update(overrides)
        result = await service.create_payment(CreatePaymentCommand(**values))
        return result.payment

    return _create
