import logging
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

from pydantic import ValidationError

from app.domain.errors import PaymentNotFoundError
from app.messaging.broker import MessagePublisher
from app.messaging.schemas import PaymentCreatedMessage
from app.messaging.topology import ATTEMPT_HEADER, RETRY_EXCHANGE, retry_queue_name
from app.services.processing import ProcessingOutcome

logger = logging.getLogger(__name__)

FIRST_ATTEMPT = 1


class Processor(Protocol):
    async def process(self, payment_id: UUID) -> ProcessingOutcome: ...


class IncomingMessage(Protocol):
    @property
    def body(self) -> bytes: ...

    @property
    def headers(self) -> Mapping[str, Any]: ...

    @property
    def message_id(self) -> str: ...

    async def ack(self) -> None: ...

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None: ...

    async def reject(self, requeue: bool = False) -> None: ...


def get_attempt(headers: Mapping[str, Any]) -> int:
    try:
        return max(int(headers.get(ATTEMPT_HEADER, FIRST_ATTEMPT)), FIRST_ATTEMPT)
    except (TypeError, ValueError):
        return FIRST_ATTEMPT


class PaymentMessageHandler:
    def __init__(
        self,
        *,
        processor: Processor,
        publisher: MessagePublisher,
        max_attempts: int,
        retry_delays_ms: list[int],
        on_processed: Callable[[], None] = lambda: None,
    ) -> None:
        if len(retry_delays_ms) != max_attempts - 1:
            raise ValueError("retry_delays_ms must contain max_attempts - 1 delays")
        self._processor = processor
        self._publisher = publisher
        self._max_attempts = max_attempts
        self._retry_delays_ms = retry_delays_ms
        self._on_processed = on_processed

    async def handle(self, message: IncomingMessage) -> None:
        try:
            await self._handle(message)
        except Exception:
            logger.exception(
                "Unexpected error while settling message, returning it to the queue",
                extra={"message_id": message.message_id},
            )
            await message.nack(requeue=True)

    async def _handle(self, message: IncomingMessage) -> None:
        attempt = get_attempt(message.headers)
        try:
            event = PaymentCreatedMessage.model_validate_json(message.body)
        except ValidationError as exc:
            logger.error(
                "Malformed message sent to DLQ",
                extra={"message_id": message.message_id, "error": str(exc)},
            )
            await message.reject(requeue=False)
            return

        log_extra = {
            "event_id": str(event.event_id),
            "payment_id": str(event.payment_id),
            "attempt": attempt,
        }
        try:
            outcome = await self._processor.process(event.payment_id)
        except PaymentNotFoundError:
            logger.error("Payment not found, message sent to DLQ", extra=log_extra)
            await message.reject(requeue=False)
            return
        except Exception as exc:
            await self._retry_or_dead_letter(message, event.event_id, attempt, exc, log_extra)
            return

        await message.ack()
        if outcome is ProcessingOutcome.PROCESSED:
            self._on_processed()

    async def _retry_or_dead_letter(
        self,
        message: IncomingMessage,
        event_id: UUID,
        attempt: int,
        error: Exception,
        log_extra: dict[str, Any],
    ) -> None:
        log_extra = {**log_extra, "error": repr(error)}
        if attempt >= self._max_attempts:
            logger.error("Processing attempts exhausted, message sent to DLQ", extra=log_extra)
            await message.reject(requeue=False)
            return

        delay_ms = self._retry_delays_ms[attempt - 1]
        try:
            await self._publisher.publish(
                message.body,
                exchange=RETRY_EXCHANGE,
                routing_key=retry_queue_name(delay_ms),
                message_id=message.message_id or str(event_id),
                headers={ATTEMPT_HEADER: attempt + 1},
            )
        except Exception:
            logger.exception("Could not schedule retry, requeueing message", extra=log_extra)
            await message.nack(requeue=True)
            return

        logger.warning(
            "Processing failed, retry scheduled",
            extra={**log_extra, "retry_in_ms": delay_ms, "next_attempt": attempt + 1},
        )
        await message.ack()
