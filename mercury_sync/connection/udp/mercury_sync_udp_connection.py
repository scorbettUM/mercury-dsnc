
import asyncio
import pickle
import time
import zlib
from collections import deque, defaultdict
from mercury_sync.connection.udp.protocols import MercurySyncUDPProtocol
from mercury_sync.models.message import Message
from mercury_sync.snowflake.snowflake_generator import SnowflakeGenerator
from typing import Tuple, Deque, Any, Dict, Coroutine, AsyncIterable


class MercurySyncUDPConnection:

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

        self._transport: asyncio.DatagramTransport = None
        self._loop = asyncio.get_event_loop()
        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]] ] = defaultdict(deque)
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_requests = deque()
        self._pending_responses = deque()

    def connect(self):
        server = self._loop.create_datagram_endpoint(
            lambda: MercurySyncUDPProtocol(
                self.read
            ),
            local_addr=(
                self.host,
                self.port
            )
        )

        transport, _ = self._loop.run_until_complete(server)
        self._transport = transport

    async def send(
        self, 
        event_name: str,
        data: Any, 
        addr: Tuple[str, int]
    ) -> Tuple[int, Any]:

        item = pickle.dumps((
            'request',
            next(self.id_generator),
            event_name,
            data
        ))

        compressed = zlib.compress(item)
        
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
    ): 

        item = pickle.dumps((
            'stream',
            next(self.id_generator),
            event_name,
            data
        ))

        compressed = zlib.compress(item)
        
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
    ):
        result: Tuple[str, int, float, Any] = pickle.loads(zlib.decompress(data))
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
                            payload
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
                            payload
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
    ):
        response: Message = await coroutine

        item = pickle.dumps(
            (
                'response', 
                next(self.id_generator),
                event_name,
                response.to_data()
            )
        )

        compressed = zlib.compress(item)
        self._transport.sendto(compressed, addr)

    async def _read_iterator(
        self,
        event_name: str,
        coroutine: AsyncIterable[Message],
        addr: Tuple[str, int]    
    ):
        async for response in coroutine:

            item = pickle.dumps(
                (
                    'response', 
                    next(self.id_generator),
                    event_name,
                    response.to_data()
                )
            )

            compressed = zlib.compress(item)

            self._transport.sendto(compressed, addr)
