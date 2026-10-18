import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

from faststream.rabbit import RabbitExchange

from app.domain.enums import Currency
from app.domain.gateway import GatewayResult, PaymentGatewayError


class FakeGateway:
    def __init__(
        self,
        *,
        success: bool = True,
        fail_transiently: bool = False,
        delay_seconds: float = 0.0,
    ) -> None:
        self.success = success
        self.fail_transiently = fail_transiently
        self.delay_seconds = delay_seconds
        self.calls: list[UUID] = []

    async def charge(
        self, *, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        self.calls.append(payment_id)
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.fail_transiently:
            raise PaymentGatewayError("gateway unavailable")
        return GatewayResult(success=self.success, decline_reason=None if self.success else "x")


@dataclass
class PublishedMessage:
    body: dict[str, Any] | bytes
    exchange: str
    routing_key: str
    message_id: str
    message_type: str | None
    headers: dict[str, Any] | None


class FakePublisher:
    def __init__(self, *, fail_on: set[int] | None = None) -> None:
        self.messages: list[PublishedMessage] = []
        self.fail_on = fail_on or set()
        self._calls = 0

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
        self._calls += 1
        if self._calls in self.fail_on:
            raise ConnectionError("broker is unavailable")
        self.messages.append(
            PublishedMessage(body, exchange.name, routing_key, message_id, message_type, headers)
        )


@dataclass
class FakeMessage:
    body: bytes
    headers: Mapping[str, Any] = field(default_factory=dict)
    message_id: str = "message-1"
    settled: list[str] = field(default_factory=list)

    async def ack(self) -> None:
        self.settled.append("ack")

    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        self.settled.append(f"nack(requeue={requeue})")

    async def reject(self, requeue: bool = False) -> None:
        self.settled.append(f"reject(requeue={requeue})")
