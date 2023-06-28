
import asyncio
import struct
from asyncio.streams import StreamReader, StreamWriter
from mercury_sync.service_discovery.dns.server.handlers.base.base_handler import BaseHandler
from mercury_sync.service_discovery.dns.server.handlers.base.handler_type import HandlerType
from typing import Optional, List


class TCPHandler:
    def __init__(
        self, 
        proxy_servers: Optional[List[str]]=None
    ):
        self.handler = BaseHandler(
            HandlerType.TCP,
            proxy_servers=proxy_servers
        )

    async def handle_tcp(
        self, 
        reader: StreamReader, 
        writer: StreamWriter
    ):
        while True:
            try:
                size, = struct.unpack('!H', await reader.readexactly(2))
            except asyncio.IncompleteReadError:
                break

            data = await reader.readexactly(size)
            async for result in self.handler.handle_dns(data, 'tcp'):
                bsize = struct.pack('!H', len(result))
                writer.write(bsize)
                writer.write(result)