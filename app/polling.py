import asyncio
import logging
from collections.abc import Awaitable, Callable

from app.domain.backoff import exponential_backoff

logger = logging.getLogger(__name__)

ERROR_BACKOFF_BASE_SECONDS = 1.0
ERROR_BACKOFF_MAX_SECONDS = 30.0


class PollingWorker:
    def __init__(
        self,
        *,
        name: str,
        step: Callable[[], Awaitable[int]],
        batch_size: int,
        poll_interval_seconds: float,
    ) -> None:
        self._name = name
        self._step = step
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._wakeup = asyncio.Event()
        self._stopping = False

    def wake(self) -> None:
        self._wakeup.set()

    def stop(self) -> None:
        self._stopping = True
        self._wakeup.set()

    async def run(self) -> None:
        logger.info("Worker started", extra={"worker": self._name})
        consecutive_errors = 0
        while not self._stopping:
            try:
                handled = await self._step()
            except Exception:
                consecutive_errors += 1
                delay = exponential_backoff(
                    consecutive_errors,
                    base_seconds=ERROR_BACKOFF_BASE_SECONDS,
                    max_seconds=ERROR_BACKOFF_MAX_SECONDS,
                ).total_seconds()
                logger.exception(
                    "Worker step failed",
                    extra={"worker": self._name, "retry_in_seconds": delay},
                )
                await self._sleep(delay)
                continue

            consecutive_errors = 0
            if handled < self._batch_size:
                await self._sleep(self._poll_interval)
        logger.info("Worker stopped", extra={"worker": self._name})

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._wakeup.wait(), timeout=seconds)
        except TimeoutError:
            pass
        self._wakeup.clear()
