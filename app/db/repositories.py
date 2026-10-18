import uuid
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, Payment, WebhookDelivery
from app.domain.enums import Currency, WebhookDeliveryStatus

ERROR_TEXT_MAX_LENGTH = 2000


def _truncate_error(error: str) -> str:
    return error[:ERROR_TEXT_MAX_LENGTH]


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_fingerprint: str,
        webhook_url: str,
    ) -> Payment | None:
        statement = (
            insert(Payment)
            .values(
                id=uuid.uuid4(),
                amount=amount,
                currency=currency,
                description=description,
                metadata_=metadata,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                webhook_url=webhook_url,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        result = await self._session.scalars(statement)
        return result.one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await self._session.scalars(statement)
        return result.one_or_none()

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        statement = (
            select(Payment)
            .where(Payment.id == payment_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self._session.scalars(statement)
        return result.one_or_none()


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def claim_batch(self, *, limit: int, lease: timedelta) -> list[OutboxEvent]:
        due = (
            select(OutboxEvent.id)
            .where(
                OutboxEvent.published_at.is_(None),
                or_(
                    OutboxEvent.next_attempt_at.is_(None),
                    OutboxEvent.next_attempt_at <= func.now(),
                ),
            )
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(due.scalar_subquery()))
            .values(attempts=OutboxEvent.attempts + 1, next_attempt_at=func.now() + lease)
            .returning(OutboxEvent)
            .execution_options(synchronize_session=False, populate_existing=True)
        )
        events = list((await self._session.scalars(statement)).all())
        return sorted(events, key=lambda event: event.created_at)

    async def mark_published(self, event_id: uuid.UUID) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.published_at.is_(None))
            .values(published_at=func.now(), next_attempt_at=None, last_error=None)
        )

    async def schedule_retry(self, event_id: uuid.UUID, *, delay: timedelta, error: str) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id, OutboxEvent.published_at.is_(None))
            .values(next_attempt_at=func.now() + delay, last_error=_truncate_error(error))
        )

    async def release(self, event_ids: Sequence[uuid.UUID]) -> None:
        if not event_ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids), OutboxEvent.published_at.is_(None))
            .values(attempts=OutboxEvent.attempts - 1, next_attempt_at=None)
        )


class WebhookDeliveryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, delivery: WebhookDelivery) -> None:
        self._session.add(delivery)

    async def get_by_payment_id(self, payment_id: uuid.UUID) -> WebhookDelivery | None:
        statement = select(WebhookDelivery).where(WebhookDelivery.payment_id == payment_id)
        result = await self._session.scalars(statement)
        return result.one_or_none()

    async def claim_batch(self, *, limit: int, lease: timedelta) -> list[WebhookDelivery]:
        due = (
            select(WebhookDelivery.id)
            .where(
                WebhookDelivery.status == WebhookDeliveryStatus.PENDING,
                or_(
                    WebhookDelivery.next_attempt_at.is_(None),
                    WebhookDelivery.next_attempt_at <= func.now(),
                ),
            )
            .order_by(WebhookDelivery.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        statement = (
            update(WebhookDelivery)
            .where(WebhookDelivery.id.in_(due.scalar_subquery()))
            .values(attempts=WebhookDelivery.attempts + 1, next_attempt_at=func.now() + lease)
            .returning(WebhookDelivery)
            .execution_options(synchronize_session=False, populate_existing=True)
        )
        return list((await self._session.scalars(statement)).all())

    async def mark_delivered(self, delivery_id: uuid.UUID, *, response_status: int | None) -> None:
        await self._session.execute(
            update(WebhookDelivery)
            .where(
                WebhookDelivery.id == delivery_id,
                WebhookDelivery.status == WebhookDeliveryStatus.PENDING,
            )
            .values(
                status=WebhookDeliveryStatus.DELIVERED,
                delivered_at=func.now(),
                next_attempt_at=None,
                last_error=None,
                last_response_status=response_status,
            )
        )

    async def schedule_retry(
        self,
        delivery_id: uuid.UUID,
        *,
        delay: timedelta,
        error: str,
        response_status: int | None,
    ) -> None:
        await self._session.execute(
            update(WebhookDelivery)
            .where(
                WebhookDelivery.id == delivery_id,
                WebhookDelivery.status == WebhookDeliveryStatus.PENDING,
            )
            .values(
                next_attempt_at=func.now() + delay,
                last_error=_truncate_error(error),
                last_response_status=response_status,
            )
        )

    async def mark_failed(
        self, delivery_id: uuid.UUID, *, error: str, response_status: int | None
    ) -> None:
        await self._session.execute(
            update(WebhookDelivery)
            .where(
                WebhookDelivery.id == delivery_id,
                WebhookDelivery.status == WebhookDeliveryStatus.PENDING,
            )
            .values(
                status=WebhookDeliveryStatus.FAILED,
                next_attempt_at=None,
                last_error=_truncate_error(error),
                last_response_status=response_status,
            )
        )

    async def release(self, delivery_ids: Sequence[uuid.UUID]) -> None:
        if not delivery_ids:
            return
        await self._session.execute(
            update(WebhookDelivery)
            .where(
                WebhookDelivery.id.in_(delivery_ids),
                WebhookDelivery.status == WebhookDeliveryStatus.PENDING,
            )
            .values(attempts=WebhookDelivery.attempts - 1, next_attempt_at=None)
        )
