from mercury_sync.service_discovery.dns.core import CacheNode, DNSMessage
from mercury_sync.service_discovery.dns.resolver import ProxyResolver, RecursiveResolver
from mercury_sync.service_discovery.dns.core.types import RecordType
from typing import Tuple, List, Optional
from .handler_type import HandlerType


class BaseHandler:

    def __init__(
        self,
        handler_type: HandlerType,
        proxy_servers: Optional[List[str]]=None
    ) -> None:
        self.handler_type = handler_type
        self.cache = CacheNode()

        self.cache.add(
            '1.0.0.127.in-addr.arpa',
            qtype=RecordType.PTR,
            data=(
                'async-dns.local',
            )
        )

        self.cache.add(
            'localhost', 
            qtype=RecordType.A, 
            data=(
                '127.0.0.1', 
            )
        )

        if proxy_servers:
            self.resolver = ProxyResolver(
                self.cache,
                proxies=proxy_servers
            )

        else:
            self.resolver = RecursiveResolver(
                self.cache
            )

    def add_to_cache(
        self,
        name: str,
        record_type: RecordType,
        address: Tuple[str, str]
    ):
        self.cache.add(
            name,
            record_type,
            address
        )

    async def handle_dns(
        self,
        data: bytes
    ):
        '''Handle DNS requests'''

        msg = DNSMessage.parse(data)
        for question in msg.qd:
            try:

                res, _ = await self.resolver.query(question.name, question.qtype)
            except Exception:
                pass

                res = None

            if res is not None:
                res.qid = msg.qid
                data = res.pack(
                    size_limit=512 if self.handler_type == HandlerType.UDP else None)  # rfc2181
                yield data

            break 