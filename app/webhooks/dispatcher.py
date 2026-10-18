import asyncio
import logging
import uuid
from datetime import timedelta

from app.db.models import WebhookDelivery
from app.db.repositories import WebhookDeliveryRepository
from app.db.session import SessionFactory
from app.domain.backoff import exponential_backoff
from app.webhooks.sender import DeliveryAttempt, WebhookSender

logger = logging.getLogger(__name__)


class WebhookDispatcher:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        sender: WebhookSender,
        batch_size: int,
        lease_seconds: float,
        max_attempts: int,
        retry_base_delay_seconds: float,
        retry_max_delay_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._sender = sender
        self._batch_size = batch_size
        self._lease = timedelta(seconds=lease_seconds)
        self._max_attempts = max_attempts
        self._retry_base_delay = retry_base_delay_seconds
        self._retry_max_delay = retry_max_delay_seconds

    async def dispatch_batch(self) -> int:
        async with self._session_factory() as session, session.begin():
            deliveries = await WebhookDeliveryRepository(session).claim_batch(
                limit=self._batch_size, lease=self._lease
            )
        if not deliveries:
            return 0

        unfinished = {delivery.id for delivery in deliveries}
        try:
            results = await asyncio.gather(
                *(self._deliver(delivery, unfinished) for delivery in deliveries),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            await asyncio.shield(self._release(list(unfinished)))
            raise
        for delivery, result in zip(deliveries, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "Webhook attempt could not be recorded; it will be retried after the lease",
                    extra={"event_id": str(delivery.id)},
                    exc_info=result,
                )
        return len(deliveries)

    async def _deliver(self, delivery: WebhookDelivery, unfinished: set[uuid.UUID]) -> None:
        attempt = await self._sender.send(
            url=delivery.url, payload=delivery.payload, event_id=delivery.id
        )
        await self._record(delivery, attempt)
        unfinished.discard(delivery.id)

    async def _record(self, delivery: WebhookDelivery, attempt: DeliveryAttempt) -> None:
        log_extra = {
            "event_id": str(delivery.id),
            "payment_id": str(delivery.payment_id),
            "attempt": delivery.attempts,
            "http_status": attempt.status_code,
        }
        async with self._session_factory() as session, session.begin():
            repository = WebhookDeliveryRepository(session)
            if attempt.delivered:
                await repository.mark_delivered(delivery.id, response_status=attempt.status_code)
                logger.info("Webhook delivered", extra=log_extra)
                return

            error = attempt.error or "unknown error"
            if attempt.retryable and delivery.attempts < self._max_attempts:
                delay = exponential_backoff(
                    delivery.attempts,
                    base_seconds=self._retry_base_delay,
                    max_seconds=self._retry_max_delay,
                )
                await repository.schedule_retry(
                    delivery.id, delay=delay, error=error, response_status=attempt.status_code
                )
                logger.warning(
                    "Webhook delivery failed, will retry",
                    extra={**log_extra, "error": error, "retry_in_seconds": delay.total_seconds()},
                )
                return

            await repository.mark_failed(
                delivery.id, error=error, response_status=attempt.status_code
            )
            logger.error("Webhook delivery failed permanently", extra={**log_extra, "error": error})

    async def _release(self, delivery_ids: list[uuid.UUID]) -> None:
        async with self._session_factory() as session, session.begin():
            await WebhookDeliveryRepository(session).release(delivery_ids)
        logger.info("Released unfinished webhook deliveries", extra={"count": len(delivery_ids)})
