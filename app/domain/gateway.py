import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from app.domain.enums import Currency

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class GatewayResult:
    success: bool
    decline_reason: str | None = None


class PaymentGatewayError(Exception):
    pass


class PaymentGateway(Protocol):
    async def charge(
        self, *, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult: ...


class EmulatedPaymentGateway:
    def __init__(
        self,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        transient_error_rate: float = 0.0,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._success_rate = success_rate
        self._transient_error_rate = transient_error_rate
        self._rng = rng or random.Random()  # noqa: S311
        self._sleep = sleep

    async def charge(
        self, *, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> GatewayResult:
        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._transient_error_rate:
            raise PaymentGatewayError("emulated gateway is temporarily unavailable")
        if self._rng.random() < self._success_rate:
            return GatewayResult(success=True)
        logger.info("Gateway declined payment", extra={"payment_id": str(payment_id)})
        return GatewayResult(success=False, decline_reason="declined_by_gateway")
