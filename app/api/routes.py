from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Response, Security, status
from sqlalchemy import text

from app.api.dependencies import get_payment_service, get_session_factory
from app.api.schemas import (
    CreatePaymentRequest,
    ErrorResponse,
    HealthResponse,
    PaymentAcceptedResponse,
    PaymentResponse,
)
from app.api.security import api_key_scheme
from app.db.models import IDEMPOTENCY_KEY_MAX_LENGTH
from app.db.session import SessionFactory
from app.services.payments import CreatePaymentCommand, PaymentService

IDEMPOTENT_REPLAY_HEADER = "Idempotent-Replayed"
IDEMPOTENCY_KEY_PATTERN = r"^[\x21-\x7E]+$"

_COMMON_ERRORS: dict[int | str, dict[str, Any]] = {
    status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse, "description": "Missing API key"},
    status.HTTP_403_FORBIDDEN: {"model": ErrorResponse, "description": "Invalid API key"},
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "model": ErrorResponse,
        "description": "Validation error",
    },
    status.HTTP_503_SERVICE_UNAVAILABLE: {
        "model": ErrorResponse,
        "description": "Database is temporarily unavailable",
    },
}

router = APIRouter(dependencies=[Security(api_key_scheme)], responses=_COMMON_ERRORS)

PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
SessionFactoryDep = Annotated[SessionFactory, Depends(get_session_factory)]


@router.post(
    "/api/v1/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    responses={
        status.HTTP_409_CONFLICT: {
            "model": ErrorResponse,
            "description": "Idempotency-Key reused with a different request body",
        }
    },
    tags=["payments"],
    summary="Create a payment (asynchronous processing)",
)
async def create_payment(
    body: CreatePaymentRequest,
    response: Response,
    service: PaymentServiceDep,
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            min_length=1,
            max_length=IDEMPOTENCY_KEY_MAX_LENGTH,
            pattern=IDEMPOTENCY_KEY_PATTERN,
            description="Unique key of the operation; repeat it when retrying the request",
        ),
    ],
) -> PaymentAcceptedResponse:
    result = await service.create_payment(
        CreatePaymentCommand(
            idempotency_key=idempotency_key,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            metadata=body.metadata,
            webhook_url=str(body.webhook_url),
        )
    )
    response.headers[IDEMPOTENT_REPLAY_HEADER] = "false" if result.created else "true"
    return PaymentAcceptedResponse.from_model(result.payment)


@router.get(
    "/api/v1/payments/{payment_id}",
    response_model=PaymentResponse,
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse}},
    tags=["payments"],
    summary="Get payment details",
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentResponse:
    return PaymentResponse.from_model(await service.get_payment(payment_id))


@router.get("/health", response_model=HealthResponse, tags=["service"], summary="Health check")
async def health(session_factory: SessionFactoryDep) -> HealthResponse:
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))
    return HealthResponse(status="ok")
