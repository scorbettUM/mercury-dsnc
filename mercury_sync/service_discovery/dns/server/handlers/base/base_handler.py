from mercury_sync.service_discovery.dns.core.cache import CacheNode
from mercury_sync.service_discovery.dns.core.dns_message import DNSMessage
from mercury_sync.service_discovery.dns.core.record import RecordType, RecordTypesMap
from mercury_sync.service_discovery.dns.resolver.proxy_resolver import ProxyResolver
from mercury_sync.service_discovery.dns.resolver.recursive_resolver import (
    RecursiveResolver
)
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import List, Optional
from .handler_type import HandlerType


class BaseHandler:

    def __init__(
        self,
        host: str,
        port: int,
        handler_type: HandlerType,
        entries: List[DNSEntry],
        proxy_servers: Optional[List[str]]=None
    ) -> None:
        self.handler_type = handler_type
        self.cache = CacheNode()
        self.types_map = RecordTypesMap()

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


        for entry in entries:
            self.cache.add(
                entry.domain_name,
                qtype=self.types_map.types_by_name.get(
                    entry.record_type
                ),
                data=[
                    str(entry) for entry in entry.domain_targets
                ]
            )

        if proxy_servers:
            self.resolver = ProxyResolver(
                host,
                port,
                self.cache,
                proxies=proxy_servers
            )

        else:
            self.resolver = RecursiveResolver(
                host,
                port,
                self.cache
            )

    def add_to_cache(
        self,
        name: str,
        record_type: RecordType,
        address: List[str]
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

        msg = DNSMessage.parse(data)

        for question in msg.qd:
            try:

                res, _ = await self.resolver.query(
                    question.name,
                    qtype=question.qtype
                )

            except Exception:
                pass

                res = None

            if res is not None:
                res.qid = msg.qid
                data = res.pack(
                    size_limit=512 if self.handler_type == HandlerType.UDP else None)  # rfc2181
                yield data

            break 