import asyncio
from mercury_sync.service_discovery.dns.resolver import BaseResolver
from mercury_sync.service_discovery.dns.server.handlers.base.base_handler import BaseHandler
from mercury_sync.service_discovery.dns.server.handlers.base.handler_type import HandlerType
from typing import Optional, List


class UDPHandler(asyncio.DatagramProtocol):
    '''DNS server handler through UDP protocol.'''
    def __init__(
        self,
        proxy_servers: Optional[List[str]]=None
    ):

        super().__init__()
        self.handler = BaseHandler(
            HandlerType.UDP,
            proxy_servers=proxy_servers
        )

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data, addr) -> None:
        asyncio.ensure_future(self.handle(data, addr))

    async def handle(self, data, addr) -> None:
        async for result in self.handler.handle_dns(data):
            self.transport.sendto(result, addr)
