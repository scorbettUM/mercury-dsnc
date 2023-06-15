import asyncio
from collections import deque
from typing import Callable, Tuple, Deque


class MercurySyncTCPServerProtocol(asyncio.Protocol):
    def __init__(
        self, 
        callback: Callable[
            [
                bytes,
                Tuple[str, int]
            ],
            bytes
        ]
    ):
        super().__init__()
        self.callback = callback
        self.transport: asyncio.Transport = None
        self._pending_responses: Deque[asyncio.Task] = deque()

    def connection_made(self, transport) -> str:
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