
import asyncio
import socket
import zstandard
from mercury_sync.connection.udp import MercurySyncUDPConnection
from mercury_sync.env import Env
from typing import Union, List, Optional
from .handlers import UDPHandler


class UDPServer(MercurySyncUDPConnection):
    def __init__(
        self,
        host: str,
        port: int,
        dns_port: int,
        instance_id: int,
        env: Env,
        proxy_servers: Optional[List[str]]=None
    ):
        super().__init__(
            host,
            port,
            instance_id,
            env
        )

        self.handler = UDPHandler(
            proxy_servers=proxy_servers
        )

        self._server: Union[asyncio.DatagramTransport, None] = None
        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self.dns_port = port

    async def start_dns_server(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ) -> None:
        
        self._loop = asyncio.get_event_loop()
        self._running = True

        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if worker_socket is None:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.udp_socket.bind((
                self.host,
                self.dns_port
            ))

            self.udp_socket.setblocking(False)

        else:
            self.udp_socket = worker_socket


        if cert_path and key_path:
            self._udp_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

            self.udp_socket = self._udp_ssl_context.wrap_socket(self.udp_socket)

        server = self._loop.create_datagram_endpoint(
            lambda: self.handler,
            sock=self.udp_socket
        )

        transport, _ = await server
        self._transport = transport

        self._cleanup_task = self._loop.create_task(self._cleanup())