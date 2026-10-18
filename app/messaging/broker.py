import asyncio
import logging
from typing import Any, Protocol

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange
from pamqp.commands import Basic

from app.config import Settings

BROKER_GRACEFUL_TIMEOUT_SECONDS = 30.0
JSON_CONTENT_TYPE = "application/json"
BROKER_LOGGER = logging.getLogger("app.messaging")


class MessageNotConfirmedError(Exception):
    pass


class MessagePublisher(Protocol):
    async def publish(
        self,
        body: dict[str, Any] | bytes,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        message_type: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None: ...


def create_broker(settings: Settings, *, prefetch_count: int | None = None) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=prefetch_count,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        graceful_timeout=BROKER_GRACEFUL_TIMEOUT_SECONDS,
        logger=BROKER_LOGGER,
    )


class ConfirmedRabbitPublisher:
    def __init__(self, broker: RabbitBroker, *, timeout_seconds: float) -> None:
        self._broker = broker
        self._timeout = timeout_seconds

    async def publish(
        self,
        body: dict[str, Any] | bytes,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        message_id: str,
        message_type: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        async with asyncio.timeout(self._timeout):
            confirmation = await self._broker.publish(
                body,
                exchange=exchange,
                routing_key=routing_key,
                message_id=message_id,
                message_type=message_type,
                headers=headers,
                content_type=JSON_CONTENT_TYPE,
                persist=True,
                mandatory=True,
                timeout=self._timeout,
            )
        if not isinstance(confirmation, Basic.Ack):
            raise MessageNotConfirmedError(f"unexpected broker confirmation: {confirmation!r}")
