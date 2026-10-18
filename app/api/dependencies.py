from fastapi import Request

from app.db.session import SessionFactory
from app.services.payments import PaymentService


def get_payment_service(request: Request) -> PaymentService:
    service: PaymentService = request.app.state.payment_service
    return service


def get_session_factory(request: Request) -> SessionFactory:
    session_factory: SessionFactory = request.app.state.session_factory
    return session_factory
