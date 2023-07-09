import asyncio
from contextlib import AbstractAsyncContextManager
from mercury_sync.models.request import Request
from mercury_sync.models.http_message import HTTPMessage
from types import TracebackType
from typing import (
    Dict, 
    Optional, 
    Type,
    Callable,
    Coroutine,
    Tuple
)



class LeakyBucketLimiter(AbstractAsyncContextManager):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_last_check",
        "_waiters",
    )

    max_rate: float
    time_period: float

    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self.max_rate = max_rate
        self.time_period = time_period
        self._rate_per_sec = max_rate / time_period
        self._level = 0.0
        self._last_check = 0.0

        self._waiters: Dict[asyncio.Task, asyncio.Future] = {}
        self._loop = asyncio.get_event_loop()

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
        self._level += amount

        return None

    async def __aenter__(self) -> None:
        await self.acquire()
        return None

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        return None