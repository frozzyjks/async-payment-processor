import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx
from sqlalchemy import select, update

from app.db.models import Payment, WebhookDelivery
from app.db.session import SessionFactory
from app.domain.enums import PaymentStatus, WebhookDeliveryStatus
from app.services.processing import PaymentProcessor, ProcessingOutcome
from app.webhooks.dispatcher import WebhookDispatcher
from app.webhooks.sender import EVENT_ID_HEADER, WebhookSender
from tests.conftest import CreatePayment
from tests.fakes import FakeGateway

HOOK_URL = "https://merchant.example.com/hook"
MAX_ATTEMPTS = 3


@pytest.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def _dispatcher(session_factory: SessionFactory, client: httpx.AsyncClient) -> WebhookDispatcher:
    return WebhookDispatcher(
        session_factory=session_factory,
        sender=WebhookSender(client, timeout_seconds=1),
        batch_size=10,
        lease_seconds=30,
        max_attempts=MAX_ATTEMPTS,
        retry_base_delay_seconds=2,
        retry_max_delay_seconds=60,
    )


async def _processed_payment(
    session_factory: SessionFactory, create_payment: CreatePayment, gateway: FakeGateway
) -> Payment:
    payment = await create_payment(webhook_url=HOOK_URL)
    processor = PaymentProcessor(
        session_factory=session_factory, gateway=gateway, lock_timeout_ms=10_000
    )
    assert await processor.process(payment.id) is ProcessingOutcome.PROCESSED
    return payment


async def _delivery(session_factory: SessionFactory) -> WebhookDelivery:
    async with session_factory() as session:
        return (await session.scalars(select(WebhookDelivery))).one()


async def _make_due(session_factory: SessionFactory) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            update(WebhookDelivery).values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )


class TestWebhookSender:
    @respx.mock
    async def test_success(self, http_client: httpx.AsyncClient) -> None:
        route = respx.post(HOOK_URL).mock(return_value=httpx.Response(204))
        event_id = uuid.uuid4()

        attempt = await WebhookSender(http_client, timeout_seconds=1).send(
            url=HOOK_URL, payload={"a": 1}, event_id=event_id
        )

        assert attempt.delivered and attempt.status_code == 204
        request = route.calls.last.request
        assert request.headers[EVENT_ID_HEADER] == str(event_id)
        assert json.loads(request.content) == {"a": 1}

    @pytest.mark.parametrize(
        ("response", "retryable"),
        [
            (httpx.Response(500), True),
            (httpx.Response(503), True),
            (httpx.Response(429), True),
            (httpx.Response(400), False),
            (httpx.Response(404), False),
            (httpx.Response(301, headers={"Location": "https://elsewhere"}), False),
        ],
    )
    @respx.mock
    async def test_unsuccessful_status(
        self, http_client: httpx.AsyncClient, response: httpx.Response, retryable: bool
    ) -> None:
        respx.post(HOOK_URL).mock(return_value=response)

        attempt = await WebhookSender(http_client, timeout_seconds=1).send(
            url=HOOK_URL, payload={}, event_id=uuid.uuid4()
        )

        assert not attempt.delivered
        assert attempt.retryable is retryable
        assert attempt.status_code == response.status_code

    @pytest.mark.parametrize(
        "error", [httpx.ConnectTimeout("t"), httpx.ReadTimeout("t"), httpx.ConnectError("c")]
    )
    @respx.mock
    async def test_network_errors_are_retryable(
        self, http_client: httpx.AsyncClient, error: Exception
    ) -> None:
        respx.post(HOOK_URL).mock(side_effect=error)

        attempt = await WebhookSender(http_client, timeout_seconds=1).send(
            url=HOOK_URL, payload={}, event_id=uuid.uuid4()
        )

        assert not attempt.delivered and attempt.retryable
        assert attempt.status_code is None


@respx.mock
async def test_dispatcher_delivers_webhook(
    session_factory: SessionFactory, create_payment: CreatePayment, http_client: httpx.AsyncClient
) -> None:
    payment = await _processed_payment(session_factory, create_payment, FakeGateway())
    route = respx.post(HOOK_URL).mock(return_value=httpx.Response(200))

    assert await _dispatcher(session_factory, http_client).dispatch_batch() == 1

    delivery = await _delivery(session_factory)
    body = json.loads(route.calls.last.request.content)
    assert delivery.status is WebhookDeliveryStatus.DELIVERED
    assert delivery.delivered_at is not None
    assert delivery.attempts == 1
    assert delivery.last_response_status == 200
    assert body["event_id"] == str(delivery.id)
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "succeeded"
    assert {"amount", "currency", "processed_at"} <= set(body)
    assert await _dispatcher(session_factory, http_client).dispatch_batch() == 0


@respx.mock
async def test_dispatcher_retries_with_backoff_and_stable_event_id(
    session_factory: SessionFactory, create_payment: CreatePayment, http_client: httpx.AsyncClient
) -> None:
    await _processed_payment(session_factory, create_payment, FakeGateway())
    route = respx.post(HOOK_URL).mock(
        side_effect=[httpx.Response(503), httpx.ConnectError("down"), httpx.Response(200)]
    )
    dispatcher = _dispatcher(session_factory, http_client)

    await dispatcher.dispatch_batch()
    first = await _delivery(session_factory)
    assert first.status is WebhookDeliveryStatus.PENDING
    assert first.attempts == 1
    assert first.last_response_status == 503
    assert first.next_attempt_at is not None
    assert first.next_attempt_at - datetime.now(UTC) > timedelta(seconds=1)
    assert await dispatcher.dispatch_batch() == 0, "retry must wait for backoff"

    await _make_due(session_factory)
    await dispatcher.dispatch_batch()
    second = await _delivery(session_factory)
    assert second.attempts == 2
    assert second.next_attempt_at is not None
    assert second.next_attempt_at - datetime.now(UTC) > timedelta(seconds=3), "backoff doubles"

    await _make_due(session_factory)
    await dispatcher.dispatch_batch()
    final = await _delivery(session_factory)
    assert final.status is WebhookDeliveryStatus.DELIVERED
    assert final.attempts == 3
    event_ids = {call.request.headers[EVENT_ID_HEADER] for call in route.calls}
    assert event_ids == {str(final.id)}


@respx.mock
async def test_dispatcher_gives_up_after_max_attempts(
    session_factory: SessionFactory, create_payment: CreatePayment, http_client: httpx.AsyncClient
) -> None:
    await _processed_payment(session_factory, create_payment, FakeGateway())
    route = respx.post(HOOK_URL).mock(return_value=httpx.Response(500))
    dispatcher = _dispatcher(session_factory, http_client)

    for _ in range(MAX_ATTEMPTS):
        await _make_due(session_factory)
        await dispatcher.dispatch_batch()

    delivery = await _delivery(session_factory)
    assert delivery.status is WebhookDeliveryStatus.FAILED
    assert delivery.attempts == MAX_ATTEMPTS
    assert route.call_count == MAX_ATTEMPTS
    await _make_due(session_factory)
    assert await dispatcher.dispatch_batch() == 0


@respx.mock
async def test_non_retryable_response_fails_immediately(
    session_factory: SessionFactory, create_payment: CreatePayment, http_client: httpx.AsyncClient
) -> None:
    await _processed_payment(session_factory, create_payment, FakeGateway())
    respx.post(HOOK_URL).mock(return_value=httpx.Response(410))

    await _dispatcher(session_factory, http_client).dispatch_batch()

    delivery = await _delivery(session_factory)
    assert delivery.status is WebhookDeliveryStatus.FAILED
    assert delivery.attempts == 1


@respx.mock
async def test_webhook_failure_never_reprocesses_payment(
    session_factory: SessionFactory, create_payment: CreatePayment, http_client: httpx.AsyncClient
) -> None:
    gateway = FakeGateway()
    payment = await _processed_payment(session_factory, create_payment, gateway)
    respx.post(HOOK_URL).mock(side_effect=httpx.ConnectError("receiver down"))

    await _dispatcher(session_factory, http_client).dispatch_batch()
    redelivered = await PaymentProcessor(
        session_factory=session_factory, gateway=gateway, lock_timeout_ms=10_000
    ).process(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)
        deliveries = (await session.scalars(select(WebhookDelivery))).all()
    assert redelivered is ProcessingOutcome.ALREADY_FINAL
    assert gateway.calls == [payment.id], "gateway must be called exactly once"
    assert stored is not None and stored.status is PaymentStatus.SUCCEEDED
    assert len(deliveries) == 1
    assert deliveries[0].status is WebhookDeliveryStatus.PENDING
    assert deliveries[0].attempts == 1
