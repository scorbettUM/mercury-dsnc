import asyncio
from collections import deque
from typing import Callable, Any, Deque


class MercurySyncTCPClientProtocol(asyncio.Protocol):
    def __init__(
        self, 
        callback: Callable[
            [Any],
            bytes
        ]
    ):
        super().__init__()
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self._pending_responses: Deque[asyncio.Task] = deque()
        self.callback = callback

        self.on_con_lost = self.loop.create_future()

    def connection_made(self, transport: asyncio.Transport) -> str:
        self.transport = transport

    def data_received(self, data: bytes):
        self._pending_responses.append(
            asyncio.create_task(
                self.callback(
                    data,
                    self.transport
                )
            )
        )

    def connection_lost(self, exc):
        self.on_con_lost.set_result(True)
