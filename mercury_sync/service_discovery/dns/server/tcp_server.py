
import asyncio
import socket
import zstandard
from mercury_sync.connection.tcp import MercurySyncTCPConnection
from mercury_sync.env import Env
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import Union, List, Optional
from .handlers import TCPHandler


class TCPServer(MercurySyncTCPConnection):
    def __init__(
        self,
        host: str,
        dns_port: int,
        instance_id: int,
        env: Env,
        entries: List[DNSEntry]=[],
        proxy_servers: Optional[List[str]]=None
    ):
        super().__init__(
            host,
            dns_port,
            instance_id,
            env
        )

        self.handler = TCPHandler(
            host,
            dns_port,
            env,
            entries,
            proxy_servers=proxy_servers
        )

        self._server: Union[asyncio.Server, None] = None
        self._waiter: Union[asyncio.Future, None] = None

    async def start_dns_server(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ):

        try:

            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self.handler.initialize()

        self._running = True
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        if self.connected is False and worker_socket is None:
            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self._server_socket.bind((self.host, self.dns_port))

            except Exception:
                pass

            self._server_socket.setblocking(False)

        elif self.connected is False:
            self._server_socket = worker_socket
            host, port = worker_socket.getsockname()
            self.host = host
            self.dns_port = port

        if self.connected is False:

            server = self._loop.create_server(
                lambda: self.handler.handle_tcp,
                sock=self._server_socket,
                ssl=self._server_ssl_context
            )

            self._server = await server
            self.connected = True

            self._cleanup_task = self._loop.create_task(self._cleanup())

    
