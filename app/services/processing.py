import logging
import uuid
from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import func, select

from app.db.models import WebhookDelivery
from app.db.repositories import PaymentRepository, WebhookDeliveryRepository
from app.db.session import SessionFactory
from app.domain.enums import PaymentStatus
from app.domain.errors import PaymentNotFoundError
from app.domain.gateway import PaymentGateway
from app.webhooks.payload import build_webhook_payload

logger = logging.getLogger(__name__)


class ProcessingOutcome(StrEnum):
    PROCESSED = "processed"
    ALREADY_FINAL = "already_final"


class PaymentProcessor:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        gateway: PaymentGateway,
        lock_timeout_ms: int,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._lock_timeout_ms = lock_timeout_ms

    async def process(self, payment_id: uuid.UUID) -> ProcessingOutcome:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                select(func.set_config("lock_timeout", f"{self._lock_timeout_ms}ms", True))
            )
            payment = await PaymentRepository(session).get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.status.is_final:
                logger.info(
                    "Payment already processed, skipping",
                    extra={"payment_id": str(payment_id), "status": payment.status.value},
                )
                return ProcessingOutcome.ALREADY_FINAL

            result = await self._gateway.charge(
                payment_id=payment.id, amount=payment.amount, currency=payment.currency
            )
            payment.status = PaymentStatus.SUCCEEDED if result.success else PaymentStatus.FAILED
            payment.processed_at = datetime.now(UTC)

            event_id = uuid.uuid4()
            WebhookDeliveryRepository(session).add(
                WebhookDelivery(
                    id=event_id,
                    payment_id=payment.id,
                    url=payment.webhook_url,
                    payload=build_webhook_payload(event_id, payment),
                )
            )

        logger.info(
            "Payment processed",
            extra={
                "payment_id": str(payment_id),
                "status": payment.status.value,
                "decline_reason": result.decline_reason,
            },
        )
        return ProcessingOutcome.PROCESSED
