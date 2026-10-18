import asyncio
import logging
import uuid
from datetime import timedelta

from app.db.models import OutboxEvent
from app.db.repositories import OutboxRepository
from app.db.session import SessionFactory
from app.domain.backoff import exponential_backoff
from app.messaging.broker import MessagePublisher
from app.messaging.topology import PAYMENTS_EXCHANGE, PAYMENTS_NEW_ROUTING_KEY

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        publisher: MessagePublisher,
        batch_size: int,
        lease_seconds: float,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._lease = timedelta(seconds=lease_seconds)
        self._retry_base_delay = retry_base_delay_seconds
        self._retry_max_delay = retry_max_delay_seconds

    async def publish_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            events = await OutboxRepository(session).claim_batch(
                limit=self._batch_size, lease=self._lease
            )

        attempted = 0
        try:
            for event in events:
                published = await self._publish_one(event)
                attempted += 1
                if not published:
                    break
        except asyncio.CancelledError:
            await asyncio.shield(self._release([event.id for event in events[attempted:]]))
            raise

        await self._release([event.id for event in events[attempted:]])
        return attempted

    async def _publish_one(self, event: OutboxEvent) -> bool:
        log_extra = {
            "event_id": str(event.id),
            "payment_id": str(event.aggregate_id),
            "attempt": event.attempts,
        }
        try:
            await self._publisher.publish(
                event.payload,
                exchange=PAYMENTS_EXCHANGE,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                message_id=str(event.id),
                message_type=event.event_type,
            )
        except Exception as exc:
            delay = exponential_backoff(
                event.attempts,
                base_seconds=self._retry_base_delay,
                max_seconds=self._retry_max_delay,
            )
            async with self._session_factory() as session, session.begin():
                await OutboxRepository(session).schedule_retry(
                    event.id, delay=delay, error=repr(exc)
                )
            logger.warning(
                "Outbox publish failed, will retry",
                extra={**log_extra, "error": repr(exc), "retry_in_seconds": delay.total_seconds()},
            )
            return False

        async with self._session_factory() as session, session.begin():
            await OutboxRepository(session).mark_published(event.id)
        logger.info("Outbox event published", extra=log_extra)
        return True

    async def _release(self, event_ids: list[uuid.UUID]) -> None:
        if not event_ids:
            return
        async with self._session_factory() as session, session.begin():
            await OutboxRepository(session).release(event_ids)
        logger.info("Released unpublished outbox events", extra={"count": len(event_ids)})
