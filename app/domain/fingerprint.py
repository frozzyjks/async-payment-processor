import hashlib
import json
from decimal import Decimal
from typing import Any

from app.domain.enums import Currency
from app.domain.money import format_amount


def compute_request_fingerprint(
    *,
    amount: Decimal,
    currency: Currency,
    description: str,
    metadata: dict[str, Any],
    webhook_url: str,
) -> str:
    canonical = json.dumps(
        {
            "amount": format_amount(amount),
            "currency": currency.value,
            "description": description,
            "metadata": metadata,
            "webhook_url": webhook_url,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
