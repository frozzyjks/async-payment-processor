import logging
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.db.models import OutboxEvent, Payment
from app.db.repositories import OutboxRepository, PaymentRepository
from app.db.session import SessionFactory
from app.domain.enums import Currency, OutboxEventType
from app.domain.errors import IdempotencyConflictError, PaymentNotFoundError
from app.domain.fingerprint import compute_request_fingerprint
from app.domain.money import normalize_amount
from app.messaging.schemas import PaymentCreatedMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


def build_payment_created_event(payment: Payment) -> OutboxEvent:
    event_id = uuid.uuid4()
    message = PaymentCreatedMessage(
        event_id=event_id, payment_id=payment.id, created_at=payment.created_at
    )
    return OutboxEvent(
        id=event_id,
        event_type=OutboxEventType.PAYMENT_CREATED.value,
        aggregate_id=payment.id,
        payload=message.model_dump(mode="json"),
    )


class PaymentService:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._session_factory = session_factory

    async def create_payment(self, command: CreatePaymentCommand) -> CreatePaymentResult:
        amount = normalize_amount(command.amount)
        fingerprint = compute_request_fingerprint(
            amount=amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            webhook_url=command.webhook_url,
        )

        async with self._session_factory() as session, session.begin():
            payment = await PaymentRepository(session).insert_if_absent(
                amount=amount,
                currency=command.currency,
                description=command.description,
                metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                request_fingerprint=fingerprint,
                webhook_url=command.webhook_url,
            )
            if payment is not None:
                OutboxRepository(session).add(build_payment_created_event(payment))

        if payment is not None:
            logger.info("Payment created", extra={"payment_id": str(payment.id)})
            return CreatePaymentResult(payment=payment, created=True)

        return await self._resolve_existing(command.idempotency_key, fingerprint)

    async def _resolve_existing(
        self, idempotency_key: str, fingerprint: str
    ) -> CreatePaymentResult:
        async with self._session_factory() as session:
            existing = await PaymentRepository(session).get_by_idempotency_key(idempotency_key)
        if existing is None:
            raise RuntimeError("Idempotency key conflicted but no payment was found")
        if existing.request_fingerprint != fingerprint:
            logger.info(
                "Idempotency key reused with different payload",
                extra={"payment_id": str(existing.id)},
            )
            raise IdempotencyConflictError(idempotency_key)
        logger.info("Idempotent replay", extra={"payment_id": str(existing.id)})
        return CreatePaymentResult(payment=existing, created=False)

    async def get_payment(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
