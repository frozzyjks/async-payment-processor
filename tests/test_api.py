import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.api.main import create_app
from app.config import Settings
from app.db.models import OutboxEvent, Payment
from app.db.session import SessionFactory, create_session_factory
from tests.conftest import TEST_API_KEY, payment_body

PAYMENTS_URL = "/api/v1/payments"


async def _post(
    client: httpx.AsyncClient, body: dict[str, Any], key: str | None = "idem-1"
) -> httpx.Response:
    headers = {"Idempotency-Key": key} if key is not None else {}
    return await client.post(PAYMENTS_URL, json=body, headers=headers)


async def _count(session_factory: SessionFactory, model: type) -> int:
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_payment_returns_202_and_stores_payment_with_outbox_event(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    response = await _post(client, payment_body())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert response.headers["Idempotent-Replayed"] == "false"

    async with session_factory() as session:
        payment = await session.get(Payment, uuid.UUID(data["payment_id"]))
        events = (await session.scalars(select(OutboxEvent))).all()
    assert payment is not None
    assert str(payment.amount) == "100.50"
    assert payment.processed_at is None
    assert len(events) == 1
    assert events[0].aggregate_id == payment.id
    assert events[0].event_type == "payment.created"
    assert events[0].payload["payment_id"] == data["payment_id"]
    assert events[0].published_at is None


@pytest.mark.parametrize(
    "amount",
    ["0", "-1", 0, -10.5, "abc", "1.001", "12345678901234567.00", "NaN", "Infinity", None],
)
async def test_invalid_amount_is_rejected(client: httpx.AsyncClient, amount: Any) -> None:
    response = await _post(client, payment_body(amount=amount))

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize("currency", ["GBP", "rub", "", None, 840])
async def test_invalid_currency_is_rejected(client: httpx.AsyncClient, currency: Any) -> None:
    response = await _post(client, payment_body(currency=currency))

    assert response.status_code == 422
    assert any(item["loc"][-1] == "currency" for item in response.json()["error"]["details"])


@pytest.mark.parametrize(
    "overrides",
    [
        {"webhook_url": "ftp://example.com/hook"},
        {"webhook_url": "not-a-url"},
        {"description": ""},
        {"metadata": ["not", "an", "object"]},
        {"unexpected": "field"},
    ],
)
async def test_invalid_body_fields_are_rejected(
    client: httpx.AsyncClient, overrides: dict[str, Any]
) -> None:
    response = await _post(client, payment_body(**overrides))

    assert response.status_code == 422


@pytest.mark.parametrize("key", [None, "", "x" * 256, "has space"])
async def test_invalid_or_missing_idempotency_key_is_rejected(
    client: httpx.AsyncClient, session_factory: SessionFactory, key: str | None
) -> None:
    response = await _post(client, payment_body(), key=key)

    assert response.status_code == 422
    assert await _count(session_factory, Payment) == 0


@pytest.mark.parametrize(
    ("headers", "expected_status", "expected_code"),
    [
        ({}, 401, "unauthorized"),
        ({"X-API-Key": "wrong-key"}, 403, "forbidden"),
        ({"X-API-Key": ""}, 401, "unauthorized"),
    ],
)
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("POST", PAYMENTS_URL, payment_body()),
        ("GET", f"{PAYMENTS_URL}/{uuid.uuid4()}", None),
        ("GET", "/health", None),
        ("GET", "/unknown-route", None),
    ],
)
async def test_requests_without_valid_api_key_are_refused(
    settings: Settings,
    session_factory: SessionFactory,
    headers: dict[str, str],
    expected_status: int,
    expected_code: str,
    method: str,
    path: str,
    body: dict[str, Any] | None,
) -> None:
    app = create_app(settings, session_factory=session_factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as anonymous:
        response = await anonymous.request(
            method, path, json=body, headers={**headers, "Idempotency-Key": "k"}
        )

    assert response.status_code == expected_status
    assert response.json() == {
        "error": {
            "code": expected_code,
            "message": response.json()["error"]["message"],
            "details": None,
        }
    }
    assert await _count(session_factory, Payment) == 0


async def test_auth_is_checked_before_body_parsing(
    settings: Settings, session_factory: SessionFactory
) -> None:
    app = create_app(settings, session_factory=session_factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as anonymous:
        response = await anonymous.post(
            PAYMENTS_URL, content=b"{broken json", headers={"Content-Type": "application/json"}
        )

    assert response.status_code == 401


async def test_openapi_documents_api_key_security(
    settings: Settings, session_factory: SessionFactory
) -> None:
    app = create_app(settings, session_factory=session_factory)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as anonymous:
        schema = (await anonymous.get("/openapi.json")).json()

    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "description": "Static API key from the API_KEY environment variable",
    }
    create_operation = schema["paths"][PAYMENTS_URL]["post"]
    assert create_operation["security"] == [{"APIKeyHeader": []}]
    assert {"401", "403", "409", "422"} <= set(create_operation["responses"])


async def test_get_existing_payment_returns_full_details(client: httpx.AsyncClient) -> None:
    created = (await _post(client, payment_body())).json()

    response = await client.get(f"{PAYMENTS_URL}/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1, "tags": ["a", "b"]},
        "status": "pending",
        "webhook_url": "https://merchant.example.com/webhooks",
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_missing_payment_returns_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid.uuid4()}")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "payment_not_found"


async def test_get_payment_with_malformed_id_returns_422(client: httpx.AsyncClient) -> None:
    response = await client.get(f"{PAYMENTS_URL}/not-a-uuid")

    assert response.status_code == 422


async def test_repeated_request_with_same_key_and_body_returns_same_payment(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    first = await _post(client, payment_body(amount="100.5"))
    second = await _post(client, payment_body(amount=100.50))

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_repeated_key_with_different_body_returns_409(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    await _post(client, payment_body())

    response = await _post(client, payment_body(amount="999.00"))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "idempotency_conflict"
    assert await _count(session_factory, Payment) == 1


async def test_concurrent_requests_with_same_key_create_single_payment(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    responses = await asyncio.gather(*(_post(client, payment_body()) for _ in range(10)))

    assert {response.status_code for response in responses} == {202}
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert sorted(response.headers["Idempotent-Replayed"] for response in responses) == (
        ["false"] + ["true"] * 9
    )
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_concurrent_requests_with_same_key_and_different_bodies(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    bodies = [payment_body(amount=f"{index + 1}.00") for index in range(5)]

    responses = await asyncio.gather(*(_post(client, body) for body in bodies))

    assert sorted(response.status_code for response in responses) == [202, 409, 409, 409, 409]
    assert await _count(session_factory, Payment) == 1


async def test_database_unavailable_returns_503(settings: Settings) -> None:
    unreachable = settings.model_copy(update={"postgres_port": 1})
    engine = create_async_engine(unreachable.database_url, poolclass=NullPool)
    app = create_app(settings, session_factory=create_session_factory(engine))
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"X-API-Key": TEST_API_KEY},
        ) as http_client:
            response = await _post(http_client, payment_body())
    finally:
        await engine.dispose()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "service_unavailable"
