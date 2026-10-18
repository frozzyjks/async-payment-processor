from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, UrlConstraints

from app.db.models import DESCRIPTION_MAX_LENGTH, URL_MAX_LENGTH, Payment
from app.domain.enums import Currency, PaymentStatus
from app.domain.money import AMOUNT_DECIMAL_PLACES, AMOUNT_MAX_DIGITS

WebhookUrl = Annotated[
    AnyHttpUrl, UrlConstraints(max_length=URL_MAX_LENGTH, allowed_schemes=["http", "https"])
]


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Order #42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "https://example.com/webhooks/payments",
                }
            ]
        },
    )

    amount: Decimal = Field(
        gt=0,
        max_digits=AMOUNT_MAX_DIGITS,
        decimal_places=AMOUNT_DECIMAL_PLACES,
        description="Strictly positive amount with at most 2 decimal places",
    )
    currency: Currency
    description: str = Field(min_length=1, max_length=DESCRIPTION_MAX_LENGTH)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: WebhookUrl


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentAcceptedResponse":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentResponse":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Any | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class HealthResponse(BaseModel):
    status: str
