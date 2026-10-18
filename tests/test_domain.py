import random
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.config import Settings
from app.domain.backoff import exponential_backoff
from app.domain.enums import Currency
from app.domain.fingerprint import compute_request_fingerprint
from app.domain.gateway import EmulatedPaymentGateway, PaymentGatewayError


def _fingerprint(**overrides: Any) -> str:
    values: dict[str, Any] = {
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "d",
        "metadata": {"a": 1, "b": {"c": 2}},
        "webhook_url": "https://example.com/",
    }
    values.update(overrides)
    return compute_request_fingerprint(**values)


def test_fingerprint_is_insensitive_to_amount_scale_and_key_order() -> None:
    assert _fingerprint() == _fingerprint(amount=Decimal("100"))
    assert _fingerprint() == _fingerprint(metadata={"b": {"c": 2}, "a": 1})


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": Decimal("100.01")},
        {"currency": Currency.USD},
        {"description": "other"},
        {"metadata": {"a": 2}},
        {"webhook_url": "https://example.org/"},
    ],
)
def test_fingerprint_changes_with_significant_fields(overrides: dict[str, Any]) -> None:
    assert _fingerprint() != _fingerprint(**overrides)


def test_exponential_backoff() -> None:
    delays = [exponential_backoff(n, base_seconds=2, max_seconds=10) for n in range(1, 5)]

    assert delays == [timedelta(seconds=s) for s in (2, 4, 8, 10)]
    with pytest.raises(ValueError):
        exponential_backoff(0, base_seconds=1, max_seconds=1)


async def test_gateway_emulator_respects_delay_and_success_rate() -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    gateway = EmulatedPaymentGateway(
        min_delay_seconds=2,
        max_delay_seconds=5,
        success_rate=0.9,
        rng=random.Random(42),
        sleep=fake_sleep,
    )
    results = [
        await gateway.charge(payment_id=uuid.uuid4(), amount=Decimal(1), currency=Currency.RUB)
        for _ in range(2000)
    ]

    success_share = sum(result.success for result in results) / len(results)
    assert all(2 <= delay <= 5 for delay in sleeps)
    assert 0.87 < success_share < 0.93


async def test_gateway_emulator_can_raise_transient_errors() -> None:
    async def no_sleep(_: float) -> None:
        return None

    gateway = EmulatedPaymentGateway(
        min_delay_seconds=0,
        max_delay_seconds=0,
        success_rate=1,
        transient_error_rate=1,
        sleep=no_sleep,
    )

    with pytest.raises(PaymentGatewayError):
        await gateway.charge(payment_id=uuid.uuid4(), amount=Decimal(1), currency=Currency.RUB)


def test_settings_build_retry_delays_and_escape_credentials() -> None:
    settings = Settings(
        api_key="k" * 16,
        postgres_password="p@ss:/word",
        rabbitmq_password="r@b/bit",
        rabbitmq_host="localhost",
        rabbitmq_port=5672,
        rabbitmq_vhost="/",
        processing_max_attempts=3,
        processing_retry_base_delay_ms=1000,
    )

    assert settings.processing_retry_delays_ms == [1000, 2000]
    assert settings.rabbitmq_url.endswith(":r%40b%2Fbit@localhost:5672/%2F")
    assert settings.database_url.password == "p@ss:/word"


def test_settings_reject_short_lease() -> None:
    with pytest.raises(ValueError, match="OUTBOX_LEASE_SECONDS"):
        Settings(
            api_key="k" * 16,
            postgres_password="p",
            rabbitmq_password="r",
            outbox_lease_seconds=5,
            rabbitmq_publish_timeout_seconds=10,
        )
