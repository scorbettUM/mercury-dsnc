import struct
import zstandard
from mercury_sync.encryption import AESGCMFernet
from mercury_sync.env import Env, load_env
from mercury_sync.service_discovery.dns.core.address import Address
from mercury_sync.service_discovery.dns.core.dns_message import DNSMessage
from mercury_sync.service_discovery.dns.core.record import RecordType
from mercury_sync.service_discovery.dns.request.connections.connection_handler import (
    ConnectionHandler
)
from typing import (
    Callable,
    Tuple,
    Any,
    Coroutine,
    Optional,
    Union
)


class TCPClient:

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
    ) -> None:
        self.record_type = record_type
        self.host = host
        self.port = port
        self.query_callback = query_callback

        env = load_env(Env.types_map())

        self._encryptor = AESGCMFernet(env)

        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None

    async def connect_async(self):
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()
 

    async def query(
        self,
        dns_request: DNSMessage,
        address: Address
    ):
        if self._compressor is None and self._decompressor is None:
            await self.connect_async()

        async with ConnectionHandler(
            *address.to_addr(),
            ssl=address.protocol == 'tcps'
        ) as conn:
            
            reader = conn.reader
            writer = conn.writer
            qdata = dns_request.pack()

            bsize = struct.pack('!H', len(qdata))

            encrypted_bsize = self._encryptor.encrypt(bsize)
            compressed_bsize = self._compressor.compress(encrypted_bsize)

            writer.write(compressed_bsize)

            encrypted_qdata = self._encryptor.encrypt(bsize)
            compressed_qdata = self._compressor.compress(encrypted_qdata)

            writer.write(compressed_qdata)

            await writer.drain()

            header = await reader.readexactly(2)
            size, = struct.unpack('!H', header)

            data = await reader.readexactly(size)

            result = DNSMessage.parse(data)

            return result

