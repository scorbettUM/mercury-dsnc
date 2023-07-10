import asyncio
from .base_limiter import BaseLimiter


class LeakyBucketLimiter(BaseLimiter):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop",
        "_last_check"
    )

    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        super().__init__(
            max_rate,
            time_period
        )

        self._level = 0.0
        self._last_check = 0.0

    def _leak(self) -> None:

        loop = asyncio.get_running_loop()

        if self._level:
            elapsed = loop.time() - self._last_check
            decrement = elapsed * self._rate_per_sec
            self._level = max(self._level - decrement, 0)            

        self._last_check = loop.time()

    def has_capacity(self, amount: float = 1) -> bool:

        self._leak()
        requested = self._level + amount

        if requested < self.max_rate:

            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break

        return self._level + amount <= self.max_rate
