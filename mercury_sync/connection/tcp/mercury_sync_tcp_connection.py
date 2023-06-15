
import asyncio
import pickle
import socket
import zlib
from collections import deque, defaultdict
from mercury_sync.snowflake.snowflake_generator import SnowflakeGenerator
from mercury_sync.models.message import Message
from typing import Tuple, Deque, Any, Dict, Coroutine, AsyncIterable
from mercury_sync.connection.tcp.protocols import (
    MercurySyncTCPClientProtocol,
    MercurySyncTCPServerProtocol
)


class MercurySyncTCPConnection:

    def __init__(
        self,
        host: str,
        port: int,
        instance_id: int
    ) -> None:

        self.id_generator = SnowflakeGenerator(instance_id)

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
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Dict[str, Deque[asyncio.Task]] = defaultdict(deque)
        self._sent_values = deque()
        self.connected = False
        self._server_socket = None
        self._stream = False
    
    def connect(self):

        if self.connected is False:

            self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._server_socket.bind((self.host, self.port))

            self._server_socket.setblocking(False)

            server = self._loop.create_server(
                lambda: MercurySyncTCPServerProtocol(
                    self.read
                ),
                sock= self._server_socket
            )

            
            self._server = self._loop.run_until_complete(server)

            self.connected = True

    async def connect_client(
        self,
        address: Tuple[str, int]
    ):

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
            port=port
        )

        self._client_transports[address] = client_transport

        return client_transport


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

        compressed = zlib.compress(item)

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

        compressed = zlib.compress(item)

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
    
        result: Tuple[
            str, 
            int, 
            float, 
            Any, 
            str, 
            int
        ] = pickle.loads(
            zlib.decompress(data)
        )

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
                            payload
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
                            payload
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

        compressed = zlib.compress(item)

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

        compressed = zlib.compress(item)
        transport.write(compressed)
        

    def close(self):
        self._stream = False

        