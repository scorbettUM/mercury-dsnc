
import asyncio
import pickle
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
        self._pending_responses = deque()
        self._sent_values = deque()
    
    def connect(self):
        server = self._loop.create_server(
            lambda: MercurySyncTCPServerProtocol(
                self.read
            ),
            host=self.host,
            port=self.port
        )

        
        self._server = self._loop.run_until_complete(server)

    async def connect_client(
        self,
        address: Tuple[str, int]
    ):

        host, port = address

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
            self._pending_responses.append(
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

        elif message_type == 'stream':

            self._pending_responses.append(
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

        else:
            print(result)
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
            

    async def close(self):
        for transport in self._client_transports.values():
            transport.close()

        self._server.close()