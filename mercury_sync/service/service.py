import random
import inspect
from mercury_sync.connection.tcp.mercury_sync_tcp_connection import MercurySyncTCPConnection
from mercury_sync.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from mercury_sync.models.message import Message
from typing import (
    Tuple, 
    Dict, 
    TypeVar, 
    Generic,
    AsyncIterable
)


T = TypeVar('T', bound=Message)
R = TypeVar('R', bound=Message)


class Service(Generic[T, R]):
    
    def __init__(
        self,
        host: str,
        port: int
    ) -> None:
        self.name = self.__class__.__name__
        self._instance_id = random.randint(0, 2**16)


        self._udp_connection = MercurySyncUDPConnection(
            host,
            port,
            self._instance_id
        )

        self._tcp_connection = MercurySyncTCPConnection(
            host,
            port + 1,
            self._instance_id
        )

        self._host_map: Dict[str, Tuple[str, int]] = {}    

        methods = inspect.getmembers(self, predicate=inspect.ismethod)

        reserved_methods = [
            'start',
            'connect',
            'send',
            'send_direct',
            'close'
        ]

        for _, method in methods:
            method_name = method.__name__

            not_internal = method_name.startswith('__') is False
            not_reserved = method_name not in reserved_methods
            is_server = hasattr(method, 'server_only')

            if not_internal and not_reserved and is_server:
                self._tcp_connection.events[method.__name__] = method
                self._udp_connection.events[method.__name__] = method

    def start(self):
        self._tcp_connection.connect()
        self._udp_connection.connect()

    async def connect(
        self,
        remote: T
    ):
        address = (remote.host, remote.port)
        self._host_map[remote.__class__.__name__] = address

        await self._tcp_connection.connect_client((
            remote.host, 
            remote.port + 1
        ))

    async def send(
        self, 
        event_name: str,
        message: T
    ) -> R:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        return await self._udp_connection.send(
            event_name,
            message.to_data(),
            address
        )
    
    async def send_direct(
        self,
        event_name: str,
        message: T
    ) -> R:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        return await self._tcp_connection.send(
            event_name,
            message.to_data(),
            address
        )
    
    async def stream(
        self,
        event_name: str,
        message: T
    ) -> AsyncIterable[R]:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        async for response in self._udp_connection.stream(
            event_name,
            message.to_data(),
            address
        ):
            yield response

    async def stream_direct(
        self,
        event_name: str,
        message: T
    ) -> AsyncIterable[R]:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        async for response in self._tcp_connection.stream(
            event_name,
            message.to_data(),
            address
        ):
            yield response
    
    async def close(self):
        await self._tcp_connection.close()
