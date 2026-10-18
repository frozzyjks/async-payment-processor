import asyncio
import logging
import signal

from app.config import get_settings
from app.db.session import create_engine, create_session_factory
from app.logging_config import configure_logging
from app.messaging.broker import ConfirmedRabbitPublisher, create_broker
from app.messaging.topology import declare_topology
from app.outbox.publisher import OutboxPublisher
from app.polling import PollingWorker

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings)
    broker = create_broker(settings)
    try:
        await broker.connect()
        await declare_topology(
            broker,
            delivery_limit=settings.consumer_delivery_limit,
            retry_delays_ms=settings.processing_retry_delays_ms,
        )
        publisher = OutboxPublisher(
            session_factory=create_session_factory(engine),
            publisher=ConfirmedRabbitPublisher(
                broker, timeout_seconds=settings.rabbitmq_publish_timeout_seconds
            ),
            batch_size=settings.outbox_batch_size,
            lease_seconds=settings.outbox_lease_seconds,
            retry_base_delay_seconds=settings.outbox_retry_base_delay_seconds,
            retry_max_delay_seconds=settings.outbox_retry_max_delay_seconds,
        )
        worker = PollingWorker(
            name="outbox-publisher",
            step=publisher.publish_batch,
            batch_size=settings.outbox_batch_size,
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
        )
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
