import asyncio
import uuid

import pytest
from sqlalchemy import select

from app.db.models import Payment, WebhookDelivery
from app.db.session import SessionFactory
from app.domain.enums import PaymentStatus, WebhookDeliveryStatus
from app.domain.errors import PaymentNotFoundError
from app.domain.gateway import PaymentGatewayError
from app.services.processing import PaymentProcessor, ProcessingOutcome
from tests.conftest import CreatePayment
from tests.fakes import FakeGateway


def _processor(session_factory: SessionFactory, gateway: FakeGateway) -> PaymentProcessor:
    return PaymentProcessor(
        session_factory=session_factory, gateway=gateway, lock_timeout_ms=10_000
    )


async def _load(session_factory: SessionFactory, payment_id: uuid.UUID) -> Payment:
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


async def _deliveries(session_factory: SessionFactory) -> list[WebhookDelivery]:
    async with session_factory() as session:
        return list((await session.scalars(select(WebhookDelivery))).all())


async def test_successful_charge_marks_payment_succeeded_and_queues_webhook(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()
    gateway = FakeGateway(success=True)

    outcome = await _processor(session_factory, gateway).process(payment.id)

    stored = await _load(session_factory, payment.id)
    [delivery] = await _deliveries(session_factory)
    assert outcome is ProcessingOutcome.PROCESSED
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert delivery.status is WebhookDeliveryStatus.PENDING
    assert delivery.url == payment.webhook_url
    assert delivery.payload == {
        "event_id": str(delivery.id),
        "event_type": "payment.processed",
        "payment_id": str(payment.id),
        "status": "succeeded",
        "amount": "250.00",
        "currency": "USD",
        "processed_at": stored.processed_at.isoformat().replace("+00:00", "Z"),
    }


async def test_declined_charge_marks_payment_failed(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()

    await _processor(session_factory, FakeGateway(success=False)).process(payment.id)

    stored = await _load(session_factory, payment.id)
    [delivery] = await _deliveries(session_factory)
    assert stored.status is PaymentStatus.FAILED
    assert stored.processed_at is not None
    assert delivery.payload["status"] == "failed"


async def test_redelivered_message_does_not_charge_twice(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()
    gateway = FakeGateway()
    processor = _processor(session_factory, gateway)

    first = await processor.process(payment.id)
    processed_at = (await _load(session_factory, payment.id)).processed_at
    second = await processor.process(payment.id)

    assert (first, second) == (ProcessingOutcome.PROCESSED, ProcessingOutcome.ALREADY_FINAL)
    assert gateway.calls == [payment.id]
    assert (await _load(session_factory, payment.id)).processed_at == processed_at
    assert len(await _deliveries(session_factory)) == 1


async def test_concurrent_duplicates_charge_once(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()
    gateway = FakeGateway(delay_seconds=0.3)

    outcomes = await asyncio.gather(
        *(_processor(session_factory, gateway).process(payment.id) for _ in range(3))
    )

    assert sorted(outcomes) == sorted(
        [ProcessingOutcome.PROCESSED] + [ProcessingOutcome.ALREADY_FINAL] * 2
    )
    assert gateway.calls == [payment.id]
    assert len(await _deliveries(session_factory)) == 1


async def test_transient_gateway_error_leaves_payment_pending(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()

    with pytest.raises(PaymentGatewayError):
        await _processor(session_factory, FakeGateway(fail_transiently=True)).process(payment.id)

    stored = await _load(session_factory, payment.id)
    assert stored.status is PaymentStatus.PENDING
    assert stored.processed_at is None
    assert await _deliveries(session_factory) == []


async def test_unknown_payment_raises_not_found(session_factory: SessionFactory) -> None:
    with pytest.raises(PaymentNotFoundError):
        await _processor(session_factory, FakeGateway()).process(uuid.uuid4())
