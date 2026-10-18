from decimal import Decimal

AMOUNT_MAX_DIGITS = 18
AMOUNT_DECIMAL_PLACES = 2
_QUANTUM = Decimal(1).scaleb(-AMOUNT_DECIMAL_PLACES)


def normalize_amount(amount: Decimal) -> Decimal:
    return amount.quantize(_QUANTUM)


def format_amount(amount: Decimal) -> str:
    return format(normalize_amount(amount), "f")
