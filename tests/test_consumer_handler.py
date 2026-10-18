import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.consumer.handler import PaymentMessageHandler, get_attempt
from app.domain.errors import PaymentNotFoundError
from app.domain.gateway import PaymentGatewayError
from app.messaging.topology import ATTEMPT_HEADER, RETRY_EXCHANGE_NAME, retry_queue_name
from app.services.processing import ProcessingOutcome
from tests.fakes import FakeMessage, FakePublisher

RETRY_DELAYS_MS = [100, 200]
MAX_ATTEMPTS = 3


class StubProcessor:
    def __init__(self, result: ProcessingOutcome | Exception) -> None:
        self.result = result
        self.calls: list[uuid.UUID] = []

    async def process(self, payment_id: uuid.UUID) -> ProcessingOutcome:
        self.calls.append(payment_id)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _body() -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "payment.created",
            "payment_id": str(uuid.uuid4()),
            "created_at": datetime.now(UTC).isoformat(),
        }
    ).encode()


def _handler(
    processor: StubProcessor, publisher: FakePublisher, processed: list[bool] | None = None
) -> PaymentMessageHandler:
    return PaymentMessageHandler(
        processor=processor,
        publisher=publisher,
        max_attempts=MAX_ATTEMPTS,
        retry_delays_ms=RETRY_DELAYS_MS,
        on_processed=(lambda: processed.append(True)) if processed is not None else lambda: None,
    )


async def test_processed_message_is_acked_and_wakes_webhooks() -> None:
    processed: list[bool] = []
    message = FakeMessage(body=_body())

    await _handler(StubProcessor(ProcessingOutcome.PROCESSED), FakePublisher(), processed).handle(
        message
    )

    assert message.settled == ["ack"]
    assert processed == [True]


async def test_already_final_payment_is_acked_without_side_effects() -> None:
    processed: list[bool] = []
    publisher = FakePublisher()
    message = FakeMessage(body=_body())

    await _handler(StubProcessor(ProcessingOutcome.ALREADY_FINAL), publisher, processed).handle(
        message
    )

    assert message.settled == ["ack"]
    assert processed == []
    assert publisher.messages == []


@pytest.mark.parametrize(
    "body",
    [b"not json", b"{}", b'{"payment_id": "not-a-uuid"}', json.dumps({"x": 1}).encode()],
)
async def test_malformed_message_goes_straight_to_dlq(body: bytes) -> None:
    processor = StubProcessor(ProcessingOutcome.PROCESSED)
    message = FakeMessage(body=body)

    await _handler(processor, FakePublisher()).handle(message)

    assert message.settled == ["reject(requeue=False)"]
    assert processor.calls == []


async def test_unknown_payment_goes_straight_to_dlq() -> None:
    message = FakeMessage(body=_body())

    await _handler(StubProcessor(PaymentNotFoundError(uuid.uuid4())), FakePublisher()).handle(
        message
    )

    assert message.settled == ["reject(requeue=False)"]


@pytest.mark.parametrize(
    ("headers", "expected_delay", "expected_next_attempt"),
    [({}, 100, 2), ({ATTEMPT_HEADER: 2}, 200, 3)],
)
async def test_transient_error_schedules_delayed_retry(
    headers: dict[str, Any], expected_delay: int, expected_next_attempt: int
) -> None:
    publisher = FakePublisher()
    message = FakeMessage(body=_body(), headers=headers)

    await _handler(StubProcessor(PaymentGatewayError("down")), publisher).handle(message)

    [retry] = publisher.messages
    assert message.settled == ["ack"]
    assert retry.exchange == RETRY_EXCHANGE_NAME
    assert retry.routing_key == retry_queue_name(expected_delay)
    assert retry.headers == {ATTEMPT_HEADER: expected_next_attempt}
    assert retry.body == message.body
    assert retry.message_id == message.message_id


async def test_exhausted_attempts_send_message_to_dlq() -> None:
    publisher = FakePublisher()
    message = FakeMessage(body=_body(), headers={ATTEMPT_HEADER: MAX_ATTEMPTS})

    await _handler(StubProcessor(PaymentGatewayError("down")), publisher).handle(message)

    assert message.settled == ["reject(requeue=False)"]
    assert publisher.messages == []


async def test_failed_retry_publication_requeues_message() -> None:
    message = FakeMessage(body=_body())

    await _handler(StubProcessor(PaymentGatewayError("down")), FakePublisher(fail_on={1})).handle(
        message
    )

    assert message.settled == ["nack(requeue=True)"]


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 1),
        ({ATTEMPT_HEADER: 3}, 3),
        ({ATTEMPT_HEADER: "2"}, 2),
        ({ATTEMPT_HEADER: "x"}, 1),
        ({ATTEMPT_HEADER: -5}, 1),
    ],
)
def test_attempt_header_parsing(headers: dict[str, Any], expected: int) -> None:
    assert get_attempt(headers) == expected


def test_handler_requires_delay_per_retry() -> None:
    with pytest.raises(ValueError, match="max_attempts - 1"):
        PaymentMessageHandler(
            processor=StubProcessor(ProcessingOutcome.PROCESSED),
            publisher=FakePublisher(),
            max_attempts=3,
            retry_delays_ms=[100],
        )
