
import asyncio
import struct
import zstandard
from asyncio.streams import StreamReader, StreamWriter
from mercury_sync.encryption import AESGCMFernet
from mercury_sync.env import Env
from mercury_sync.service_discovery.dns.server.handlers.base.base_handler import (
    BaseHandler
)
from mercury_sync.service_discovery.dns.server.handlers.base.handler_type import (
    HandlerType
)
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import Optional, List, Union


class TCPHandler:
    def __init__(
        self, 
        host: str,
        port: int,
        env: Env,
        entries: List[DNSEntry],
        proxy_servers: Optional[List[str]]=None
    ):
        self.handler = BaseHandler(
            host,
            port,
            HandlerType.TCP,
            entries,
            proxy_servers=proxy_servers
        )

        self._encryptor = AESGCMFernet(env)
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None

    def initialize(self):
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

    def add_entry(
        self,
        entry: DNSEntry
    ):
        record_type = self.handler.types_map.types_by_name.get(entry.record_type)

        self.handler.add_to_cache(
            entry.to_domain(),
            record_type,
            entry.to_data()
        )

    def get_nameserver_addresses(
        self,
        domain_name: str
    ):
        nameservers = self.handler.resolver._get_nameservers(domain_name)
        return nameservers.data

    async def handle_tcp(
        self, 
        reader: StreamReader, 
        writer: StreamWriter
    ):
        while True:
            try:

                size_bytes = await reader.readexactly(2)
                decompressed_size_bytes = self._decompressor.decompress(size_bytes)

                size, = struct.unpack(
                    '!H', 
                    self._encryptor.decrypt(decompressed_size_bytes)
                )

            except asyncio.IncompleteReadError:
                break

            data_bytes = await reader.readexactly(size)
            decompressed_data = self._decompressor.decompress(data_bytes)
            decrypted_data = self._encryptor.decrypt(decompressed_data)

            async for result in self.handler.handle_dns(decrypted_data):
                bsize = struct.pack('!H', len(result))
                writer.write(bsize)
                writer.write(result)