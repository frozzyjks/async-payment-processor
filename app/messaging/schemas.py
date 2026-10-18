from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.domain.enums import OutboxEventType


class PaymentCreatedMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID
    event_type: Literal[OutboxEventType.PAYMENT_CREATED] = OutboxEventType.PAYMENT_CREATED
    payment_id: UUID
    created_at: datetime
