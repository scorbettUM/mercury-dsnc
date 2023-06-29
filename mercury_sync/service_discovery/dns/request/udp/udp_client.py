from __future__ import annotations
import asyncio
import socket
import zstandard
from collections import deque
from mercury_sync.connection.addresses import SubnetRange
from mercury_sync.encryption import AESGCMFernet
from mercury_sync.env import Env, load_env
from mercury_sync.connection.udp.protocols import MercurySyncUDPProtocol
from mercury_sync.service_discovery.dns.core.address import Address
from mercury_sync.service_discovery.dns.core.dns_message import DNSMessage
from mercury_sync.service_discovery.dns.core.random import RandomIDGenerator
from mercury_sync.service_discovery.dns.core.record import RecordType
from typing import (
    Union, 
    Tuple, 
    Deque,
    Callable,
    Any,
    Coroutine,
    Optional
)



class UDPClient:

    def __init__(
        self, 
        record_type: RecordType, 
        host: str,
        port: int,
        query_callback: Optional[
            Callable[
                [str],
                Coroutine[
                    Any,
                    Any,
                    Tuple[DNSMessage, bool]
                ]
            ]
        ]=None
    ):
        self.record_type = record_type
        self.host = host
        self.port = port
        self.query_callback = query_callback

        env = load_env(Env.types_map())

        self.random_id_generator = RandomIDGenerator()
        self._transport: Union[asyncio.DatagramTransport, None] = None
        self._pending: Deque[asyncio.Future] = deque()
        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._subnet = SubnetRange(self.host)
        self._encryptor = AESGCMFernet(env)
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None


    async def connect_async(self):
        
        self._loop = asyncio.get_event_loop()
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        family = socket.AF_INET
        if self.record_type is RecordType.AAAA:
            family = socket.AF_INET6

        for host_ip in self._subnet:
            try:
                self._transport, _ = await self._loop.create_datagram_endpoint(
                    lambda: MercurySyncUDPProtocol(
                        self.read
                    ),
                    family=family,
                    local_addr=(
                        host_ip,
                        self.port
                    )
                )

                break

            except OSError:
                pass

            except asyncio.TimeoutError:
                pass

    async def query(
        self, 
        dns_request: DNSMessage, 
        addr: Address
    ):
        if self._transport is None:
            await self.connect_async()

        qid = self.random_id_generator.generate()
        dns_request.qid = qid

        host, port = addr.to_addr()
        if port is None:
            port = self.port

        request = dns_request.pack()

        encrypted_request = self._encryptor.encrypt(request)
        compressed = self._compressor.compress(encrypted_request)

        self._transport.sendto(
            compressed, 
            (host, port)
        )

        waiter = self._loop.create_future()
        self._pending.append(
            waiter
        )

        data = await waiter
        decompressed = self._decompressor.decompress(data)
        decrypted = self._encryptor.decrypt(decompressed)

        return DNSMessage.parse(decrypted)

    def read(
        self,
        data: bytes,
        address: Tuple[str, int]
    ):

        if len(self._pending) > 0:
            waiter = self._pending.pop()
            waiter.set_result(data)

    def close(self):
        self._transport.abort()
