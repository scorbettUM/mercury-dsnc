
import asyncio
import pickle
import socket
import ssl
import zstandard
from collections import deque, defaultdict
from dtls import do_patch
from mercury_sync.connection.udp.protocols import MercurySyncUDPProtocol
from mercury_sync.encryption import AESGCMFernet
from mercury_sync.env import Env
from mercury_sync.env.time_parser import TimeParser
from mercury_sync.models.message import Message
from mercury_sync.snowflake.snowflake_generator import SnowflakeGenerator
from typing import (
    Tuple, 
    Deque, 
    Any, 
    Dict, 
    Coroutine, 
    AsyncIterable,
    Optional,
    Union
)

do_patch()


class MercurySyncUDPConnection:

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

        self._transport: asyncio.DatagramTransport = None
        self._loop = asyncio.get_event_loop()
        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]] ] = defaultdict(deque)
        self.parsers: Dict[str, Message] = {}
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Deque[asyncio.Task] = deque()

        self._udp_cert_path: Union[str, None] = None
        self._udp_key_path: Union[str, None] = None
        self._udp_ssl_context: Union[ssl.SSLContext, None] = None

        self._encryptor = AESGCMFernet(env)
        self._semaphore = asyncio.Semaphore(env.MERCURY_SYNC_MAX_CONCURRENCY)
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()
        
        self._running = False
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time


    def connect(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> None:
        
        self._running = True

        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if cert_path and key_path:
            self._udp_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

            udp_socket = self._udp_ssl_context.wrap_socket(udp_socket)

        udp_socket.bind((
            self.host,
            self.port
        ))

        udp_socket.setblocking(False)

        server = self._loop.create_datagram_endpoint(
            lambda: MercurySyncUDPProtocol(
                self.read
            ),
            sock=udp_socket
        )

        transport, _ = self._loop.run_until_complete(server)
        self._transport = transport

        self._cleanup_task = self._loop.create_task(self._cleanup())

    def _create_udp_ssl_context(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext: 
        
        if self._udp_cert_path is None:
            self._udp_cert_path = cert_path

        if self._udp_key_path is None:
            self._udp_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
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
    
    async def _cleanup(self):
        while self._running:
            await asyncio.sleep(self._cleanup_interval)

            for pending in list(self._pending_responses):
                if pending.done() or pending.cancelled():
                    self._pending_responses.pop()
    
    async def send(
        self, 
        event_name: str,
        data: Any, 
        addr: Tuple[str, int]
    ) -> Tuple[int, Dict[str, Any]]:

        item = pickle.dumps((
            'request',
            next(self.id_generator),
            event_name,
            data
        ), protocol=pickle.HIGHEST_PROTOCOL)

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)
        
        self._transport.sendto(compressed, addr)

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
        addr: Tuple[str, int]
    ) -> AsyncIterable[Tuple[int, Dict[str, Any]]]: 

        item = pickle.dumps((
            'stream',
            next(self.id_generator),
            event_name,
            data
        ), protocol=pickle.HIGHEST_PROTOCOL)

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)
        
        self._transport.sendto(compressed, addr)

        waiter = self._loop.create_future()
        self._waiters[event_name].append(waiter)

        await waiter

        for item in self.queue[event_name]:
            (
                _,
                shard_id,
                _,
                response_data,
                _, 
                _
            ) = item

            yield(
                shard_id,
                response_data
            )

        self.queue.clear()

    def read(
        self,
        data: bytes, 
        addr: Tuple[str, int]
    ) -> None:
        
        decrypted = self._encryptor.decrypt(
            self._decompressor.decompress(data)
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
            payload
        ) = result

        incoming_host, incoming_port = addr


        if message_type == 'request':
            self._pending_responses.append(
                asyncio.create_task(
                    self._read(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        addr
                    )
                )
            )
            
        elif message_type == 'stream':
            self._pending_responses.append(
                asyncio.create_task(
                    self._read_iterator(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        addr
                    )
                )
            )

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
        addr: Tuple[str, int]
    ) -> Coroutine[Any, Any, None]:
        response: Message = await coroutine

        item = pickle.dumps(
            (
                'response', 
                next(self.id_generator),
                event_name,
                response.to_data()
            ),
            protocol=pickle.HIGHEST_PROTOCOL
        )

        encrypted_message = self._encryptor.encrypt(item)
        compressed = self._compressor.compress(encrypted_message)

        self._transport.sendto(compressed, addr)

    async def _read_iterator(
        self,
        event_name: str,
        coroutine: AsyncIterable[Message],
        addr: Tuple[str, int]    
    ) -> Coroutine[Any, Any, None]:
        async for response in coroutine:

            item = pickle.dumps(
                (
                    'response', 
                    next(self.id_generator),
                    event_name,
                    response.to_data()
                ),
                protocol=pickle.HIGHEST_PROTOCOL
            )

            encrypted_message = self._encryptor.encrypt(item)
            compressed = self._compressor.compress(encrypted_message)

            self._transport.sendto(compressed, addr)

    async def close(self) -> None:
        self._running = False
        await self._cleanup_task