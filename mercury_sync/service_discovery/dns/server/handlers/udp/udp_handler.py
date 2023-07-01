import asyncio
import zstandard
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


class UDPHandler(asyncio.DatagramProtocol):
    '''DNS server handler through UDP protocol.'''
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        entries: List[DNSEntry],
        proxy_servers: Optional[List[str]]=None
    ):

        super().__init__()
        self.handler = BaseHandler(
            host,
            port,
            HandlerType.UDP,
            entries,
            proxy_servers=proxy_servers
        )

        self._encryptor = AESGCMFernet(env)
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None

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
    
    def initialize(self):
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

    def get_nameserver_addresses(
        self,
        domain_name: str
    ):
        nameservers = self.handler.resolver._get_nameservers(domain_name)
        return nameservers.data

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data, addr) -> None:
        data = self._encryptor.decrypt(
            self._decompressor.decompress(data)
        )

        asyncio.create_task(self.handle(data, addr))

    async def handle(self, data, addr) -> None:
        async for result in self.handler.handle_dns(data):
            encrypted_result = self._encryptor.encrypt(result)
            self.transport.sendto(
                self._compressor.compress(encrypted_result), 
                addr
            )
