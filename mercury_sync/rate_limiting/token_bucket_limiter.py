import asyncio
from types import TracebackType
from typing import (
    Optional, 
    Type
)
from .base_limiter import BaseLimiter


class TokenBucketLimiter(BaseLimiter):

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

        self._level = max_rate
        self._last_check = self._loop.time()

    def has_capacity(self, amount: float = 1) -> bool:

        if self._level < self.max_rate:
            current_time = self._loop.time()
            delta = self._rate_per_sec * (current_time - self._last_check)
            self._level = min(self.max_rate, self._level + delta)
            self._last_check = current_time

        requested_amount = self._level - amount
        if requested_amount > 0 or self._level >= self.max_rate:
            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break


        return amount < self._level
    
    async def acquire(
        self, 
        amount: float = 1
    ) -> None:
        
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")


        task = asyncio.current_task(
            loop=self._loop
        )

        assert task is not None

        while not self.has_capacity(amount):

            fut = self._loop.create_future()
            self._waiters[task] = fut

            try:

                await asyncio.wait_for(
                    asyncio.shield(fut), 
                    timeout=(1 / self._rate_per_sec * amount)
                )

            except asyncio.TimeoutError:
                pass

            fut.cancel()

        self._waiters.pop(task, None)
        self._level -= amount

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        return None