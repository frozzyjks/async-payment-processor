from enum import StrEnum


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self is not PaymentStatus.PENDING


class WebhookDeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


class OutboxEventType(StrEnum):
    PAYMENT_CREATED = "payment.created"


class WebhookEventType(StrEnum):
    PAYMENT_PROCESSED = "payment.processed"
