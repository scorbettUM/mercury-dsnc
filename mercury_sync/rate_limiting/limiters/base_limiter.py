import asyncio
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import (
    Dict, 
    Optional, 
    Type
)



class BaseLimiter(AbstractAsyncContextManager):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop"
    )

    def __init__(self, max_rate: float, time_period: float = 60) -> None:
        self.max_rate = max_rate
        self.time_period = time_period
        self._rate_per_sec = max_rate / time_period
        self._level = 0.0

        self._waiters: Dict[asyncio.Task, asyncio.Future] = {}
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    def has_capacity(self, amount: float = 1) -> bool:
        raise NotImplementedError('Err. - has_capacity() is not implemented on BaseLimiter')

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

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        return None