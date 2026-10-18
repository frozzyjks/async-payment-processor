import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import aio_pika
import httpx
import pytest
import respx
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy import select

from app.config import Settings
from app.consumer.handler import PaymentMessageHandler
from app.db.models import OutboxEvent, Payment, WebhookDelivery
from app.db.session import SessionFactory
from app.domain.enums import Currency, PaymentStatus, WebhookDeliveryStatus
from app.messaging.broker import ConfirmedRabbitPublisher, create_broker
from app.messaging.schemas import PaymentCreatedMessage
from app.messaging.topology import (
    ATTEMPT_HEADER,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_ROUTING_KEY,
    dead_letter_queue,
    declare_topology,
    payments_new_queue,
)
from app.outbox.publisher import OutboxPublisher
from app.services.payments import CreatePaymentCommand, PaymentService
from app.services.processing import PaymentProcessor
from app.webhooks.dispatcher import WebhookDispatcher
from app.webhooks.sender import WebhookSender
from tests.fakes import FakeGateway

pytestmark = pytest.mark.integration

RETRY_DELAYS_MS = [100, 200]
MAX_ATTEMPTS = 3
DELIVERY_LIMIT = 5
WAIT_TIMEOUT_SECONDS = 15.0
HOOK_URL = "https://merchant.example.com/hook"


def _management_url(settings: Settings) -> str:
    return os.environ.get("RABBITMQ_MANAGEMENT_URL", f"http://{settings.rabbitmq_host}:15672")


@pytest.fixture
async def vhost_settings(settings: Settings) -> AsyncIterator[Settings]:
    vhost = quote(settings.rabbitmq_vhost, safe="")
    auth = (settings.rabbitmq_user, settings.rabbitmq_password.get_secret_value())
    async with httpx.AsyncClient(base_url=_management_url(settings), auth=auth) as api:
        try:
            await api.delete(f"/api/vhosts/{vhost}")
            (await api.put(f"/api/vhosts/{vhost}")).raise_for_status()
            (
                await api.put(
                    f"/api/permissions/{vhost}/{quote(settings.rabbitmq_user, safe='')}",
                    json={"configure": ".*", "write": ".*", "read": ".*"},
                )
            ).raise_for_status()
        except httpx.HTTPError as exc:
            pytest.skip(f"RabbitMQ management API is not available: {exc!r}")
        yield settings
        await api.delete(f"/api/vhosts/{vhost}")


async def _start_consumer(
    settings: Settings, session_factory: SessionFactory, gateway: FakeGateway
) -> RabbitBroker:
    broker = create_broker(settings, prefetch_count=10)
    handler = PaymentMessageHandler(
        processor=PaymentProcessor(
            session_factory=session_factory, gateway=gateway, lock_timeout_ms=10_000
        ),
        publisher=ConfirmedRabbitPublisher(broker, timeout_seconds=5),
        max_attempts=MAX_ATTEMPTS,
        retry_delays_ms=RETRY_DELAYS_MS,
    )

    @broker.subscriber(
        payments_new_queue(DELIVERY_LIMIT), PAYMENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL
    )
    async def handle(message: RabbitMessage) -> None:
        await handler.handle(message)

    await broker.connect()
    await declare_topology(broker, delivery_limit=DELIVERY_LIMIT, retry_delays_ms=RETRY_DELAYS_MS)
    await broker.start()
    return broker


@pytest.fixture
async def consumer_factory(
    vhost_settings: Settings, session_factory: SessionFactory
) -> AsyncIterator[Callable[[FakeGateway], Awaitable[RabbitBroker]]]:
    brokers: list[RabbitBroker] = []

    async def _factory(gateway: FakeGateway) -> RabbitBroker:
        try:
            broker = await _start_consumer(vhost_settings, session_factory, gateway)
        except (OSError, aio_pika.exceptions.AMQPError) as exc:
            pytest.skip(f"RabbitMQ is not available: {exc!r}")
        brokers.append(broker)
        return broker

    yield _factory
    for broker in brokers:
        await broker.stop()


async def _create_payment(session_factory: SessionFactory, key: str = "it-1") -> Payment:
    result = await PaymentService(session_factory).create_payment(
        CreatePaymentCommand(
            idempotency_key=key,
            amount=Decimal("42.00"),
            currency=Currency.EUR,
            description="integration",
            metadata={},
            webhook_url=HOOK_URL,
        )
    )
    return result.payment


async def _wait_for(predicate: Callable[[], Awaitable[bool]]) -> None:
    async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
        while not await predicate():
            await asyncio.sleep(0.1)


async def _payment_status(session_factory: SessionFactory, payment_id: uuid.UUID) -> PaymentStatus:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment.status


async def _get_dead_letter(broker: RabbitBroker) -> aio_pika.abc.AbstractIncomingMessage:
    queue = await broker.declare_queue(dead_letter_queue())
    async with asyncio.timeout(WAIT_TIMEOUT_SECONDS):
        while True:
            message = await queue.get(no_ack=True, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.1)


def _message_for(payment: Payment) -> dict[str, Any]:
    return PaymentCreatedMessage(
        event_id=uuid.uuid4(), payment_id=payment.id, created_at=datetime.now(UTC)
    ).model_dump(mode="json")


async def test_payment_flows_from_outbox_through_broker_to_webhook(
    vhost_settings: Settings,
    session_factory: SessionFactory,
    consumer_factory: Callable[[FakeGateway], Awaitable[RabbitBroker]],
) -> None:
    gateway = FakeGateway()
    broker = await consumer_factory(gateway)
    payment = await _create_payment(session_factory)

    outbox = OutboxPublisher(
        session_factory=session_factory,
        publisher=ConfirmedRabbitPublisher(broker, timeout_seconds=5),
        batch_size=10,
        lease_seconds=30,
        retry_base_delay_seconds=1,
        retry_max_delay_seconds=10,
    )
    assert await outbox.publish_batch() == 1

    async def processed() -> bool:
        return (await _payment_status(session_factory, payment.id)).is_final

    await _wait_for(processed)
    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is not None
    assert gateway.calls == [payment.id]

    async with httpx.AsyncClient() as client, respx.mock(assert_all_called=True) as mock:
        route = mock.post(HOOK_URL).mock(return_value=httpx.Response(200))
        dispatcher = WebhookDispatcher(
            session_factory=session_factory,
            sender=WebhookSender(client, timeout_seconds=1),
            batch_size=10,
            lease_seconds=30,
            max_attempts=3,
            retry_base_delay_seconds=1,
            retry_max_delay_seconds=10,
        )
        assert await dispatcher.dispatch_batch() == 1
    webhook = json.loads(route.calls.last.request.content)
    async with session_factory() as session:
        delivery = await session.get(WebhookDelivery, uuid.UUID(webhook["event_id"]))
    assert webhook["payment_id"] == str(payment.id)
    assert delivery is not None and delivery.status is WebhookDeliveryStatus.DELIVERED


async def test_message_goes_to_dlq_after_three_failed_attempts(
    session_factory: SessionFactory,
    consumer_factory: Callable[[FakeGateway], Awaitable[RabbitBroker]],
) -> None:
    gateway = FakeGateway(fail_transiently=True)
    broker = await consumer_factory(gateway)
    payment = await _create_payment(session_factory)

    await ConfirmedRabbitPublisher(broker, timeout_seconds=5).publish(
        _message_for(payment),
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        message_id="dlq-test",
    )
    dead = await _get_dead_letter(broker)

    assert gateway.calls == [payment.id] * MAX_ATTEMPTS
    assert dead.headers[ATTEMPT_HEADER] == MAX_ATTEMPTS
    assert dead.message_id == "dlq-test"
    death = dead.headers["x-death"][0]  # type: ignore[index]
    assert death["reason"] == "rejected"
    assert death["queue"] == "payments.new"
    assert json.loads(dead.body)["payment_id"] == str(payment.id)
    assert await _payment_status(session_factory, payment.id) is PaymentStatus.PENDING


async def test_malformed_message_goes_to_dlq_without_retries(
    consumer_factory: Callable[[FakeGateway], Awaitable[RabbitBroker]],
) -> None:
    gateway = FakeGateway()
    broker = await consumer_factory(gateway)

    await ConfirmedRabbitPublisher(broker, timeout_seconds=5).publish(
        b'{"garbage": true}',
        exchange=PAYMENTS_EXCHANGE,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        message_id="bad",
    )
    dead = await _get_dead_letter(broker)

    assert dead.message_id == "bad"
    assert ATTEMPT_HEADER not in (dead.headers or {})
    assert gateway.calls == []


async def test_duplicate_messages_charge_once(
    session_factory: SessionFactory,
    consumer_factory: Callable[[FakeGateway], Awaitable[RabbitBroker]],
) -> None:
    gateway = FakeGateway(delay_seconds=0.2)
    broker = await consumer_factory(gateway)
    payment = await _create_payment(session_factory)
    publisher = ConfirmedRabbitPublisher(broker, timeout_seconds=5)
    body = _message_for(payment)

    for _ in range(3):
        await publisher.publish(
            body,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENTS_NEW_ROUTING_KEY,
            message_id=body["event_id"],
        )

    async def processed() -> bool:
        return (await _payment_status(session_factory, payment.id)).is_final

    await _wait_for(processed)
    await asyncio.sleep(1)
    queue = await broker.declare_queue(payments_new_queue(DELIVERY_LIMIT))
    assert gateway.calls == [payment.id]
    assert queue.declaration_result.message_count == 0


async def test_unroutable_message_is_not_confirmed(
    consumer_factory: Callable[[FakeGateway], Awaitable[RabbitBroker]],
) -> None:
    broker = await consumer_factory(FakeGateway())

    with pytest.raises(aio_pika.exceptions.PublishError):
        await ConfirmedRabbitPublisher(broker, timeout_seconds=5).publish(
            {"x": 1}, exchange=PAYMENTS_EXCHANGE, routing_key="no.such.queue", message_id="x"
        )
