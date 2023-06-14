import random
import inspect
from mercury_sync.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from mercury_sync.models.message import Message
from typing import Tuple, Dict, Callable, Coroutine


class UDPService:
    
    def __init__(
        self,
        name: str,
        host: str,
        port: int
    ) -> None:
        self.name = name
        self._instance_id = random.randint(0, 2**16)
        self._connection = MercurySyncUDPConnection(
            host,
            port,
            instance_id=self._instance_id
        )

        self._host_map: Dict[str, Tuple[str, int]]        

    def connect(self):
        self._connection.connect()

    def add_connection(
        self,
        name: str,
        host: str,
        port: int
    ):
        self._host_map[name] = (host, port)

    async def send(
        self, 
        message: Message
    ):
        (host, port)  = self._host_map.get(message.name)
        address = (
            host,
            port
        )

        return await self._connection.send(
            self.name,
            message.data,
            address
        )
    
