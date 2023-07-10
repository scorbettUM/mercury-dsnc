from __future__ import annotations
import asyncio
import ipaddress
import socket
import ssl
import traceback
import zstandard
from collections import deque, defaultdict
from mercury_sync.env import Env
from mercury_sync.env.time_parser import TimeParser
from mercury_sync.connection.base.connection_type import ConnectionType
from mercury_sync.models.http_message import HTTPMessage
from mercury_sync.models.http_request import HTTPRequest
from mercury_sync.models.request import Request
from mercury_sync.rate_limiting import Limiter
from pydantic import BaseModel
from typing import (
    Tuple, 
    Union, 
    Optional, 
    Deque, 
    Dict, 
    List, 
    Callable
)
from .mercury_sync_tcp_connection import MercurySyncTCPConnection
from .protocols import MercurySyncTCPClientProtocol


class MercurySyncHTTPConnection(MercurySyncTCPConnection):

    def __init__(
        self, 
        host: str, 
        port: int,
        instance_id: int, 
        env: Env,
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
        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY

        self.connection_type = ConnectionType.HTTP
        self._is_server = env.MERCURY_SYNC_USE_HTTP_SERVER
        self._use_encryption = env.MERCURY_SYNC_USE_HTTP_MSYNC_ENCRYPTION

        self._supported_handlers: Dict[str, Dict[str, str]] = defaultdict(dict)
        self._response_parsers: Dict[
            Tuple[str, int],
            Callable[
                [BaseModel],
                str
            ]
        ] = {}

        self._limiter = Limiter(env)

        rate_limit_strategy = env.MERCURY_SYNC_HTTP_RATE_LIMIT_STRATEGY
        self._rate_limiting_enabled = rate_limit_strategy != "none"

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

        if self._compressor is None and self._decompressor is None:
            self._compressor = zstandard.ZstdCompressor()
            self._decompressor = zstandard.ZstdDecompressor()
        
       
        if cert_path and key_path:
            self._client_ssl_context = self._create_client_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        elif is_ssl:
            self._client_ssl_context = self._create_general_client_ssl_context(
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

    def _create_general_client_ssl_context(
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
        event_name: str,
        data: HTTPRequest, 
        address: Tuple[str, int]
    ):
        async with self._semaphore:

            connections = self._connections.get(address)
            if connections is None:

                connections = await self.connect_client(
                    address,
                    cert_path=self._client_cert_path,
                    key_path=self._client_key_path,
                    is_ssl='https' in data.url
                )

                self._connections[address] = connections

            client_transport = connections.pop()

            result: Union[bytes, None] = None

            try:

                encoded_request = data.prepare_request()
                encrypted_request = self._encryptor.encrypt(encoded_request)
                compressed_request = self._compressor.compress(encrypted_request)

                client_transport.write(compressed_request)

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
    
    async def send_request(
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
        
        if self._is_server:
            self._pending_responses.append(
                asyncio.create_task(
                    self._route_request(
                        data,
                        transport
                    )
                )
            )

        elif bool(self._waiters):

            waiter = self._waiters.pop()
            waiter.set_result(
                HTTPRequest.parse(data)
            )

    async def _route_request(
        self, 
        data: bytes,
        transport: asyncio.Transport
    ):
        

        if self._use_encryption:
            decompressed_data = self._decompressor.decompress(data)
            data = self._encryptor.decrypt(decompressed_data)
        
        request_data = data.split(b'\r\n')
        method, path, request_type = request_data[0].decode().split(' ')

        handler_key = f'{method}_{path}'

        try:

            handler = self.events[handler_key]

            query: Union[str, None] = None
            if '?' in path:
                path, query = path.split('?')


            request = Request(
                path,
                method,
                query,
                request_data,
                model=self.parsers.get(handler_key)
            )

            if self._rate_limiting_enabled:

                ip_address, _ = transport.get_extra_info('peername')

                await self._limiter.limit(
                    ipaddress.ip_address(ip_address),
                    request,
                    limit=handler.limit
                )

            response_info: Tuple[
                Union[str, None],
                int
            ] = await handler(request)

            (
                response_data, 
                status_code
            ) = response_info

            response_key = f'{handler_key}_{status_code}'

            head_line = f'HTTP/1.1 {status_code} OK'
            
            encoded_data: str = ''

            response_parser = self._response_parsers.get(response_key)

            if response_parser:

                parsed_response = response_parser(response_data)

                encoded_data = parsed_response

                content_length = len(parsed_response)
                headers = f'content-length: {content_length}'

            elif response_data:
                encoded_data = response_data

                content_length = len(response_data)
                headers = f'content-length: {content_length}'

            else:
                headers = 'content-length: 0'

            response_headers = handler.response_headers
            if response_headers:
                for key in response_headers:
                    headers = f'{headers}\r\n{key}: {response_headers[key]}'

            response_bytes = f'{head_line}\r\n{headers}\r\n\r\n{encoded_data}'.encode()

            if self._use_encryption:
                encrypted_data = self._encryptor.encrypt(response_bytes)
                response_bytes = self._compressor.compress(encrypted_data)

            transport.write(response_bytes)

        except KeyError:
            
            if self._supported_handlers.get(request.path) is None:
            
                not_found_response = HTTPMessage(
                    path=request.path,
                    status=404,
                    error='Not Found',
                    protocol=request_type,
                    method=request.method
                )

                transport.write(not_found_response.prepare_response())

            elif self._supported_handlers[request.path].get(request.method) is None:

                method_not_allowed_response = HTTPMessage(
                    path=request.path,
                    status=405,
                    error='Method Not Allowed',
                    protocol=request_type,
                    method=request.method
                )

                transport.write(method_not_allowed_response.prepare_response())

        except Exception:
            print(traceback.format_exc())

            if transport.is_closing() is False:

                server_error_respnse = HTTPMessage(
                    path=request.path,
                    status=500,
                    error='Internal Error',
                    protocol=request_type,
                    method=request.method
                )

                transport.write(server_error_respnse.prepare_response())
