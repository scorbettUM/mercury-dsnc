
import asyncio
import pickle
import zlib
from collections import deque, defaultdict
from mercury_sync.snowflake.snowflake_generator import SnowflakeGenerator
from typing import Tuple, Deque, Any, Dict, Coroutine
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

        self._client_transport: asyncio.Transport = None
        self._server: asyncio.Server = None
        self._loop = asyncio.get_event_loop()
        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]] ] = defaultdict(deque)
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses = deque()

    async def send(
        self, 
        event_name: bytes,
        data: bytes, 
        addr: Tuple[str, int]
    ) -> str:

        host, port = addr
        
        if self._client_transport is None:

            self._client_transport, _ = await self._loop.create_connection(
                lambda: MercurySyncTCPClientProtocol(
                    self.read
                ),
                host=host,
                port=port
            )

        item = pickle.dumps((
            'request',
            next(self.id_generator),
            event_name,
            data,
            self.host,
            self.port
        ))

        compressed = zlib.compress(item)

        self._client_transport.write(compressed)

        waiter = self._loop.create_future()
        self._waiters[event_name].append(waiter)

        await waiter

        return self.queue[event_name].pop()
    
    def connect(self):
        server = self._loop.create_server(
            lambda: MercurySyncTCPServerProtocol(
                self.read
            ),
            host=self.host,
            port=self.port
        )

        
        self._server = self._loop.run_until_complete(server)

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
        response = await coroutine

        item = pickle.dumps(
            (
                'response', 
                next(self.id_generator),
                event_name,
                response, 
                self.host,
                self.port
            )
        )

        compressed = zlib.compress(item)

        transport.write(compressed)


async def run(client: MercurySyncTCPConnection):
    response = await client.send(
        'test',
        'Hello world!',
        (
            '0.0.0.0',
            1123
        )
    )

    print(response)

async def example_response(
    shard_id: int,
    payload     
): 
    return 'Alright!'


async def example_response_two(
    shard_id: int,
    payload     
): 
    return 'Okay!'



if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = MercurySyncTCPConnection(
        host='0.0.0.0',
        port=1123,
        instance_id=1
    )

    server.events['test'] = example_response

    server.connect()


    client = MercurySyncTCPConnection(
        host='0.0.0.0',
        port=1124,
        instance_id=2
    )

    client.events['test'] = example_response_two

    client.connect()

    loop.run_until_complete(run(client))
    