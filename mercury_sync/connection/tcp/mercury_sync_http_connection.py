from __future__ import annotations
import asyncio
import socket
import ssl
from collections import deque, defaultdict
from mercury_sync.env import Env
from mercury_sync.connection.base.connection_type import ConnectionType
from mercury_sync.models.http_request import HTTPRequest
from typing import Tuple, Union, Optional, Deque, Dict, List
from .mercury_sync_tcp_connection import MercurySyncTCPConnection
from .protocols import MercurySyncTCPClientProtocol


class MercurySyncHTTPConnection(MercurySyncTCPConnection):

    def __init__(
        self, 
        host: str, 
        port: int,
        instance_id: int, 
        env: Env,
        pool_size: int=10
    ) -> None:
        super().__init__(
            host, 
            port, 
            instance_id, 
            env
        )

        self._waiters: Deque[asyncio.Future] = deque()
        self._connections: Dict[str, List[asyncio.Transport]] = defaultdict(list)
        self._http_socket: Union[socket.socket, None] = None
        self._hostnames: Dict[Tuple[str, int], str] = {}
        self._max_concurrency = pool_size

        self.connection_type = ConnectionType.HTTP

    async def connect_client(
        self,
        address: Tuple[str, int],
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None,
        is_ssl: bool=False,
        hostname: str=None,
    ) -> None:
        
        self._hostnames[address] = hostname
        
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)
        
       
        if (cert_path and key_path) or is_ssl:
            self._client_ssl_context = self._create_client_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        last_error: Union[Exception, None] = None

        for _ in range(self._tcp_connect_retries):

            try:

                self._connections[address] = await asyncio.gather(*[
                    self._connect_client(
                        address,
                        hostname=hostname,
                        worker_socket=worker_socket
                    ) for _ in range(self._max_concurrency)
                ])

                return
            
            except ConnectionRefusedError as connection_error:
                last_error = connection_error

            await asyncio.sleep(1)

        if last_error:
            raise last_error

    def _create_client_ssl_context(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
    ):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        return ctx

        
    async def _connect_client(
        self,
        address: Tuple[str, int],
        hostname: str=None,
        worker_socket: Optional[socket.socket]=None,
    ) -> asyncio.Transport:
        
        self._loop = asyncio.get_event_loop()

        if worker_socket is None:

            http_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            http_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            await self._loop.run_in_executor(None, http_socket.connect, address)

            http_socket.setblocking(False)

        else:
            http_socket = worker_socket
 
        transport, _ = await self._loop.create_connection(
            lambda: MercurySyncTCPClientProtocol(
                self.read
            ),
            sock=http_socket,
            server_hostname=hostname,
            ssl=self._client_ssl_context
        )

        return transport
    
    async def send(
        self, 
        data: HTTPRequest, 
        address: Tuple[str, int]
    ):
        async with self._semaphore:

            encoded_request = data.prepare_request()

            connections = self._connections.get(address)
            client_transport = connections.pop()

            result: Union[bytes, None] = None

            try:

                client_transport.write(encoded_request)

                waiter = self._loop.create_future()
                self._waiters.append(waiter)
                
                result = await waiter

            except Exception:
                self._connections[address].append(
                    await self._connect_client(
                        (
                            self.host,
                            self.port
                        ),
                        hostname=self._hostnames.get(address)
                    )
                )

            self._connections[address].append(client_transport)

            return result

    def read(
        self, 
        data: bytes, 
        transport: asyncio.Transport
    ) -> None:

        if bool(self._waiters):

            waiter = self._waiters.pop()
            waiter.set_result(
                HTTPRequest.parse(data)
            )