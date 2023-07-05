from mercury_sync.discovery.dns.core.record import (
    RecordType,
    RecordTypesMap
)
from typing import Literal, Optional, List, Tuple, Callable, Union
from .proxy_resolver import ProxyResolver
from .recursive_resolver import RecursiveResolver


Proxy = List[
    Tuple[
        Union[
            Callable[
                [str],
                bool
            ],
            str,
            None
        ], 
        str
    ]
]


class DNSResolver:

    def __init__(
        self,
        host: str,
        port: int,
        resolver: Literal["proxy", "recursive"]="proxy",
        proxies: Optional[
            List[Proxy]
        ]=None, 
        max_tick: int=5,
        query_timeout: float = 3.0,
        request_timeout: float = 5.0
    ) -> None:
        
        if resolver == "proxy":
            self.resolver = ProxyResolver(
                host,
                port,
                proxies=proxies,
                query_timeout=query_timeout,
                request_timeout=request_timeout
            )

        else:
            self.resolver = RecursiveResolver(
                host,
                port,
                max_tick=max_tick,
                query_timeout=query_timeout,
                request_timeout=request_timeout
            )

        self.types_map = RecordTypesMap()

    def set_proxies(
        self,
        proxies: List[Proxy]
    ):
        if isinstance(self.resolver, ProxyResolver):
            self.resolver.set_proxies(proxies)

    def download_common(self):
        if isinstance(self.resolver, RecursiveResolver):
            self.resolver.load_nameserver_cache()

    async def query(
        self,
        domain_name: str,
        record_type: RecordType=RecordType.SRV,
        skip_cache: bool=False
    ):
        
        record_type = self.types_map.types_by_name.get(
            record_type
        )

        return await self.resolver.query(
            domain_name,
            record_type=record_type,
            skip_cache=skip_cache
        )