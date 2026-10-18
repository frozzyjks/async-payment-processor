import logging
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import httpx

logger = logging.getLogger(__name__)

EVENT_ID_HEADER = "X-Webhook-Event-Id"
RETRYABLE_CLIENT_ERRORS = frozenset(
    {HTTPStatus.REQUEST_TIMEOUT, HTTPStatus.TOO_EARLY, HTTPStatus.TOO_MANY_REQUESTS}
)


@dataclass(frozen=True, slots=True)
class DeliveryAttempt:
    delivered: bool
    retryable: bool
    status_code: int | None = None
    error: str | None = None


def _classify_response(response: httpx.Response) -> DeliveryAttempt:
    status = response.status_code
    if response.is_success:
        return DeliveryAttempt(delivered=True, retryable=False, status_code=status)
    retryable = status >= HTTPStatus.INTERNAL_SERVER_ERROR or status in RETRYABLE_CLIENT_ERRORS
    return DeliveryAttempt(
        delivered=False,
        retryable=retryable,
        status_code=status,
        error=f"receiver responded with HTTP {status}",
    )


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, *, timeout_seconds: float) -> None:
        self._client = client
        self._timeout = timeout_seconds

    async def send(
        self, *, url: str, payload: dict[str, Any], event_id: uuid.UUID
    ) -> DeliveryAttempt:
        try:
            response = await self._client.post(
                url,
                json=payload,
                headers={EVENT_ID_HEADER: str(event_id)},
                timeout=self._timeout,
            )
        except httpx.TimeoutException as exc:
            return DeliveryAttempt(delivered=False, retryable=True, error=f"timeout: {exc!r}")
        except httpx.HTTPError as exc:
            return DeliveryAttempt(delivered=False, retryable=True, error=f"network error: {exc!r}")
        except httpx.InvalidURL as exc:
            return DeliveryAttempt(delivered=False, retryable=False, error=f"invalid url: {exc}")
        return _classify_response(response)
