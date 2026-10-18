from datetime import timedelta


def exponential_backoff(attempt: int, *, base_seconds: float, max_seconds: float) -> timedelta:
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    delay = min(base_seconds * 2 ** (attempt - 1), max_seconds)
    return timedelta(seconds=delay)
