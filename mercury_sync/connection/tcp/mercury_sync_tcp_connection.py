
import asyncio
import base64
import pickle
import socket
import ssl
import zlib
from collections import deque, defaultdict
from cryptography.fernet import Fernet
from mercury_sync.env import Env
from mercury_sync.models.message import Message
from mercury_sync.snowflake.snowflake_generator import SnowflakeGenerator
from typing import (
    Tuple, 
    Deque, 
    Any, 
    Dict, 
    Coroutine, 
    AsyncIterable,
    Union,
    Optional
)
from mercury_sync.connection.tcp.protocols import (
    MercurySyncTCPClientProtocol,
    MercurySyncTCPServerProtocol
)


class MercurySyncTCPConnection:

    def __init__(
        self,
        host: str,
        port: int,
        instance_id: int,
        env: Env
    ) -> None:

        self.id_generator = SnowflakeGenerator(instance_id)
        self.env = env

        self.host = host
        self.port = port

        self.events: Dict[
            str,
            Coroutine
        ] = {}

        self._client_transports: Dict[str, asyncio.Transport] = {}
        self._server: asyncio.Server = None
        self._loop = asyncio.get_event_loop()
        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]] ] = defaultdict(deque)
        self.parsers: Dict[str, Message] = {}
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Dict[str, Deque[asyncio.Task]] = defaultdict(deque)

        self._sent_values = deque()
        self.connected = False
        self._server_socket = None
        self._stream = False
        
        self._client_key_path: Union[str, None] = None
        self._client_cert_path: Union[str, None] = None

        self._server_key_path: Union[str, None] = None
        self._server_cert_path: Union[str, None] = None 
        
        self._client_ssl_context: Union[ssl.SSLContext, None] = None
        self._server_ssl_context: Union[ssl.SSLContext, None] = None
        
        fernet_key = base64.urlsafe_b64encode(
            env.MERURY_SYNC_AUTH_SECRET.encode().ljust(32)[:32]
        )

        self._encrypter = Fernet(fernet_key)

    def connect(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        if self.connected is False:

            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind((self.host, self.port))

            self._server_socket.setblocking(False)

            server = self._loop.create_server(
                lambda: MercurySyncTCPServerProtocol(
                    self.read
                ),
                sock=self._server_socket,
                ssl=self._server_ssl_context
            )

            self._server = self._loop.run_until_complete(server)

            self.connected = True

    def _create_server_ssl_context(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext:
        
        if self._server_cert_path is None:
            self._server_cert_path = cert_path

        if self._server_key_path is None:
            self._server_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers('ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384')

        return ssl_ctx

    async def connect_client(
        self,
        address: Tuple[str, int],
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        
        if cert_path and key_path:
            self._client_ssl_context = self._create_client_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        host, port = address
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        await self._loop.run_in_executor(None, tcp_socket.connect, address)

        tcp_socket.setblocking(False)

        client_transport, _ = await self._loop.create_connection(
            lambda: MercurySyncTCPClientProtocol(
                self.read
            ),
            host=host,
            port=port,
            ssl=self._client_ssl_context
        )

        self._client_transports[address] = client_transport

        return client_transport
    
    def _create_client_ssl_context(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext:
        
        if self._client_cert_path is None:
            self._client_cert_path = cert_path

        if self._client_key_path is None:
            self._client_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers('ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384')

        return ssl_ctx

    async def send(
        self, 
        event_name: bytes,
        data: bytes, 
        address: Tuple[str, int]
    ) -> str:
        
        client_transport = self._client_transports.get(address)

        item = pickle.dumps((
            'request',
            next(self.id_generator),
            event_name,
            data,
            self.host,
            self.port
        ))

        encrypted_message = self._encrypter.encrypt(item)

        compressed = zlib.compress(encrypted_message)

        client_transport.write(compressed)

        waiter = self._loop.create_future()
        self._waiters[event_name].append(waiter)

        await waiter

        (
            _,
            shard_id,
            _,
            response_data,
            _, 
            _
        ) = self.queue[event_name].pop()

        return (
            shard_id,
            response_data
        )
    
    async def stream(
        self, 
        event_name: str,
        data: Any, 
        address: Tuple[str, int]
    ): 
        

        client_transport = self._client_transports.get(address)


        if self._stream is False:
            item = pickle.dumps((
                'stream_connect',
                next(self.id_generator),
                event_name,
                data,
                self.host,
                self.port
            ))


        else:
            item = pickle.dumps((
                'stream',
                next(self.id_generator),
                event_name,
                data,
                self.host,
                self.port
            ))

        encrypted_message = self._encrypter.encrypt(item)
        compressed = zlib.compress(encrypted_message)

        client_transport.write(compressed)

        waiter = self._loop.create_future()
        self._waiters[event_name].append(waiter)

        await waiter

        if self._stream is False:

            self.queue[event_name].pop()

            self._stream = True

            item = pickle.dumps((
                'stream',
                next(self.id_generator),
                event_name,
                data,
                self.host,
                self.port
            ))

            compressed = zlib.compress(item)

            client_transport.write(compressed)

            waiter = self._loop.create_future()
            self._waiters[event_name].append(waiter)

            await waiter


        while len(self.queue[event_name]) > 0 and self._stream:

            (
                _,
                shard_id,
                _,
                response_data,
                _, 
                _
            ) = self.queue[event_name].pop()
        
            yield(
                shard_id,
                response_data
            )


        self.queue.clear()

    def read(
        self,
        data: bytes,
        transport: asyncio.Transport
    ):
        decrypted = self._encrypter.decrypt(
            zlib.decompress(data)
        )

        result: Tuple[
            str, 
            int, 
            float, 
            Any, 
            str, 
            int
        ] = pickle.loads(decrypted)

        (
            message_type, 
            shard_id, 
            event_name,
            payload, 
            incoming_host, 
            incoming_port
        ) = result

        if message_type == 'request':
            self._pending_responses[event_name].append(
                asyncio.create_task(
                    self._read(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        transport
                    )
                )
            )

        elif message_type == "stream_connect":

            self.queue[event_name].append((
                message_type, 
                shard_id,
                event_name,
                payload, 
                incoming_host,
                incoming_port
            ))

            self._pending_responses[event_name].append(
                asyncio.create_task(
                    self._initialize_stream(
                        event_name,
                        transport
                    )
                )
            )

            event_waiter = self._waiters[event_name]

            if len(event_waiter) > 0:
                waiter = event_waiter.pop()
                waiter.set_result(None)

        elif message_type == 'stream' or message_type == "stream_connect":

            self.queue[event_name].append((
                message_type, 
                shard_id,
                event_name,
                payload, 
                incoming_host,
                incoming_port
            ))

            self._pending_responses[event_name].append(
                asyncio.create_task(
                    self._read_iterator(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        transport
                    )
                )
            )

            event_waiter = self._waiters[event_name]

            if len(event_waiter) > 0:
                waiter = event_waiter.pop()
                waiter.set_result(None)


        else:
            self.queue[event_name].append((
                message_type, 
                shard_id,
                event_name,
                payload, 
                incoming_host,
                incoming_port
            ))

                
            event_waiter = self._waiters[event_name]

            if len(event_waiter) > 0:
                waiter = event_waiter.pop()
                waiter.set_result(None)

    async def _read(
        self,
        event_name: str,
        coroutine: Coroutine,
        transport: asyncio.Transport
    ):
        response: Message = await coroutine

        item = pickle.dumps(
            (
                'response', 
                next(self.id_generator),
                event_name,
                response.to_data(), 
                self.host,
                self.port
            )
        )

        encrypted_message = self._encrypter.encrypt(item)
        compressed = zlib.compress(encrypted_message)

        transport.write(compressed)

    async def _read_iterator(
        self,
        event_name: str,
        coroutine: AsyncIterable[Message],
        transport: asyncio.Transport       
    ):
  
        async for response in coroutine:
            item = pickle.dumps(
                (
                    'response', 
                    next(self.id_generator),
                    event_name,
                    response.to_data(), 
                    self.host,
                    self.port
                )
            )

            compressed = zlib.compress(item)
            transport.write(compressed)

    async def _initialize_stream(
        self,
        event_name: str,
        transport: asyncio.Transport            
    ):
        message = Message()
        item = pickle.dumps(
            (
                'response', 
                next(self.id_generator),
                event_name,
                message.to_data(), 
                self.host,
                self.port
            )
        )

        encrypted_message = self._encrypter.encrypt(item)
        compressed = zlib.compress(encrypted_message)

        transport.write(compressed)
        
    def close(self):
        self._stream = False

        