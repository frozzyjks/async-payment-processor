import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.db.models import Payment
from app.domain.enums import Currency, PaymentStatus, WebhookEventType
from app.domain.money import format_amount


class WebhookPayload(BaseModel):
    event_id: uuid.UUID
    event_type: WebhookEventType = WebhookEventType.PAYMENT_PROCESSED
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: str
    currency: Currency
    processed_at: datetime


def build_webhook_payload(event_id: uuid.UUID, payment: Payment) -> dict[str, Any]:
    if payment.processed_at is None:
        raise ValueError("Webhook can only be built for a processed payment")
    return WebhookPayload(
        event_id=event_id,
        payment_id=payment.id,
        status=payment.status,
        amount=format_amount(payment.amount),
        currency=payment.currency,
        processed_at=payment.processed_at,
    ).model_dump(mode="json")
