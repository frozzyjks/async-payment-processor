from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update

from app.db.models import OutboxEvent
from app.db.repositories import OutboxRepository
from app.db.session import SessionFactory
from app.messaging.topology import PAYMENTS_EXCHANGE_NAME, PAYMENTS_NEW_ROUTING_KEY
from app.outbox.publisher import OutboxPublisher
from tests.conftest import CreatePayment
from tests.fakes import FakePublisher


def _publisher(session_factory: SessionFactory, fake: FakePublisher) -> OutboxPublisher:
    return OutboxPublisher(
        session_factory=session_factory,
        publisher=fake,
        batch_size=10,
        lease_seconds=60,
        retry_base_delay_seconds=1,
        retry_max_delay_seconds=60,
    )


async def _events(session_factory: SessionFactory) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(
            (await session.scalars(select(OutboxEvent).order_by(OutboxEvent.created_at))).all()
        )


async def test_event_is_published_and_marked(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    payment = await create_payment()
    fake = FakePublisher()

    assert await _publisher(session_factory, fake).publish_batch() == 1

    [event] = await _events(session_factory)
    [message] = fake.messages
    assert event.published_at is not None
    assert event.attempts == 1
    assert message.exchange == PAYMENTS_EXCHANGE_NAME
    assert message.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert message.message_id == str(event.id)
    assert message.body == event.payload
    assert message.body["payment_id"] == str(payment.id)
    assert await _publisher(session_factory, fake).publish_batch() == 0


async def test_publish_failure_schedules_retry_with_backoff(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    await create_payment()
    publisher = _publisher(session_factory, FakePublisher(fail_on={1}))

    await publisher.publish_batch()

    [event] = await _events(session_factory)
    assert event.published_at is None
    assert event.attempts == 1
    assert event.last_error is not None
    assert "broker is unavailable" in event.last_error
    assert event.next_attempt_at is not None
    assert event.next_attempt_at > datetime.now(UTC)
    assert await publisher.publish_batch() == 0, "event must wait for its backoff"


async def test_failure_releases_rest_of_batch(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    for _ in range(3):
        await create_payment()
    fake = FakePublisher(fail_on={1})

    attempted = await _publisher(session_factory, fake).publish_batch()

    events = await _events(session_factory)
    assert attempted == 1
    assert events[0].attempts == 1 and events[0].next_attempt_at is not None
    for released in events[1:]:
        assert released.attempts == 0
        assert released.next_attempt_at is None
        assert released.published_at is None
    assert await _publisher(session_factory, FakePublisher()).publish_batch() == 2


async def test_retry_after_backoff_publishes_event(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    await create_payment()
    await _publisher(session_factory, FakePublisher(fail_on={1})).publish_batch()
    async with session_factory() as session, session.begin():
        await session.execute(
            update(OutboxEvent).values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    fake = FakePublisher()
    assert await _publisher(session_factory, fake).publish_batch() == 1

    [event] = await _events(session_factory)
    assert event.published_at is not None
    assert event.attempts == 2
    assert event.last_error is None


async def test_crash_after_publish_before_mark_republishes_after_lease(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    await create_payment()
    async with session_factory() as session, session.begin():
        claimed = await OutboxRepository(session).claim_batch(limit=10, lease=timedelta(seconds=60))
    assert len(claimed) == 1

    assert await _publisher(session_factory, FakePublisher()).publish_batch() == 0, (
        "a live lease must protect the event from a second publisher"
    )

    async with session_factory() as session, session.begin():
        await session.execute(
            update(OutboxEvent).values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    fake = FakePublisher()
    assert await _publisher(session_factory, fake).publish_batch() == 1
    assert fake.messages[0].message_id == str(claimed[0].id)


async def test_concurrent_publishers_claim_disjoint_events(
    session_factory: SessionFactory, create_payment: CreatePayment
) -> None:
    for _ in range(6):
        await create_payment()

    async with session_factory() as first, session_factory() as second:
        async with first.begin():
            first_batch = await OutboxRepository(first).claim_batch(
                limit=4, lease=timedelta(seconds=60)
            )
            async with second.begin():
                second_batch = await OutboxRepository(second).claim_batch(
                    limit=10, lease=timedelta(seconds=60)
                )

    first_ids = {event.id for event in first_batch}
    second_ids = {event.id for event in second_batch}
    assert len(first_ids) == 4
    assert len(second_ids) == 2
    assert first_ids.isdisjoint(second_ids)
