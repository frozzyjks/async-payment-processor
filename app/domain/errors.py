from uuid import UUID


class IdempotencyConflictError(Exception):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__("Idempotency-Key was already used with a different request body")
        self.idempotency_key = idempotency_key


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id
