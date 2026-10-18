from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.db.models import OutboxEvent, Payment
from app.db.session import SessionFactory
from app.domain.enums import Currency
from app.services import payments as payments_module
from app.services.payments import CreatePaymentCommand, PaymentService

COMMAND = CreatePaymentCommand(
    idempotency_key="atomic-1",
    amount=Decimal("10.00"),
    currency=Currency.EUR,
    description="atomicity",
    metadata={},
    webhook_url="https://merchant.example.com/hook",
)


async def test_payment_and_outbox_event_share_one_transaction(
    session_factory: SessionFactory,
) -> None:
    payment = (await PaymentService(session_factory).create_payment(COMMAND)).payment

    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.aggregate_id == payment.id
    assert event.created_at == payment.created_at


async def test_failed_outbox_write_rolls_back_payment(
    session_factory: SessionFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_event(payment: Payment) -> OutboxEvent:
        return OutboxEvent(event_type=None, aggregate_id=payment.id, payload={})

    monkeypatch.setattr(payments_module, "build_payment_created_event", broken_event)

    with pytest.raises(IntegrityError):
        await PaymentService(session_factory).create_payment(COMMAND)

    async with session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(OutboxEvent))
    assert payments == 0
    assert events == 0
