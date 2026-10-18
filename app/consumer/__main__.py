import asyncio
import contextlib
import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit.annotations import RabbitMessage

from app.config import Settings, get_settings
from app.consumer.handler import PaymentMessageHandler
from app.db.session import create_engine, create_session_factory
from app.domain.gateway import EmulatedPaymentGateway
from app.logging_config import configure_logging
from app.messaging.broker import BROKER_LOGGER, ConfirmedRabbitPublisher, create_broker
from app.messaging.topology import PAYMENTS_EXCHANGE, declare_topology, payments_new_queue
from app.polling import PollingWorker
from app.services.processing import PaymentProcessor
from app.webhooks.dispatcher import WebhookDispatcher
from app.webhooks.sender import WebhookSender

logger = logging.getLogger(__name__)

WORKER_STOP_TIMEOUT_SECONDS = 15.0


def build_app(settings: Settings) -> FastStream:
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    broker = create_broker(settings, prefetch_count=settings.consumer_prefetch_count)
    http_client = httpx.AsyncClient(follow_redirects=False)

    processor = PaymentProcessor(
        session_factory=session_factory,
        gateway=EmulatedPaymentGateway(
            min_delay_seconds=settings.gateway_min_delay_seconds,
            max_delay_seconds=settings.gateway_max_delay_seconds,
            success_rate=settings.gateway_success_rate,
            transient_error_rate=settings.gateway_transient_error_rate,
        ),
        lock_timeout_ms=settings.db_lock_timeout_ms,
    )
    dispatcher = WebhookDispatcher(
        session_factory=session_factory,
        sender=WebhookSender(http_client, timeout_seconds=settings.webhook_timeout_seconds),
        batch_size=settings.webhook_batch_size,
        lease_seconds=settings.webhook_lease_seconds,
        max_attempts=settings.webhook_max_attempts,
        retry_base_delay_seconds=settings.webhook_retry_base_delay_seconds,
        retry_max_delay_seconds=settings.webhook_retry_max_delay_seconds,
    )
    webhook_worker = PollingWorker(
        name="webhook-dispatcher",
        step=dispatcher.dispatch_batch,
        batch_size=settings.webhook_batch_size,
        poll_interval_seconds=settings.webhook_poll_interval_seconds,
    )
    handler = PaymentMessageHandler(
        processor=processor,
        publisher=ConfirmedRabbitPublisher(
            broker, timeout_seconds=settings.rabbitmq_publish_timeout_seconds
        ),
        max_attempts=settings.processing_max_attempts,
        retry_delays_ms=settings.processing_retry_delays_ms,
        on_processed=webhook_worker.wake,
    )

    @broker.subscriber(
        payments_new_queue(settings.consumer_delivery_limit),
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
    )
    async def handle_payment_created(message: RabbitMessage) -> None:
        await handler.handle(message)

    app = FastStream(broker, logger=BROKER_LOGGER)
    background: list[asyncio.Task[None]] = []

    @app.on_startup
    async def prepare_topology() -> None:
        await broker.connect()
        await declare_topology(
            broker,
            delivery_limit=settings.consumer_delivery_limit,
            retry_delays_ms=settings.processing_retry_delays_ms,
        )

    @app.after_startup
    async def start_webhook_worker() -> None:
        background.append(asyncio.create_task(webhook_worker.run()))

    @app.on_shutdown
    async def stop_webhook_worker() -> None:
        webhook_worker.stop()
        for task in background:
            try:
                await asyncio.wait_for(task, timeout=WORKER_STOP_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.warning("Webhook worker did not stop in time, cancelled")
            except Exception:
                logger.exception("Webhook worker finished with error")

    @app.after_shutdown
    async def release_resources() -> None:
        with contextlib.suppress(Exception):
            await http_client.aclose()
        await engine.dispose()

    return app


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    await build_app(settings).run()


if __name__ == "__main__":
    asyncio.run(main())
