
from mercury_sync.env import Env, load_env
from mercury_sync.service_discovery.dns.core.config import core_config
from mercury_sync.service_discovery.dns.core.dns_message import (
    DNSMessage,
    REQUEST
)
from mercury_sync.service_discovery.dns.core.nameservers import NameServers
from mercury_sync.service_discovery.dns.core.record import (
    Record,
    RecordType,
    RecordTypesMap
)
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import Callable, List, Tuple, Optional, Union
from .base_resolver import BaseResolver
from .memoizer import Memoizer



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

NSPair = Tuple[
    Union[
        Callable[
            [str],
            bool
        ],
        str,
        None
    ],
    NameServers
]


class ProxyResolver(BaseResolver):
    '''Proxy DNS resolver.

    Resolve hostnames from another recursive DNS server instead of root servers.
    '''
    name = 'ProxyResolver'
    default_nameservers = core_config['default_nameservers']
    memoizer = Memoizer()

    def __init__(
        self, 
        host: str,
        port: int,
        *args, 
        proxies: Optional[List[Proxy]]=None, 
        **kwargs
    ):
        super().__init__(
            host,
            port,
            *args, 
            **kwargs
        )

        if proxies is None:
            proxies = self.default_nameservers

        self.types_map = RecordTypesMap()
        self.ns_pairs: List[NSPair] = []

        nameserver_pairs = self.set_proxies(proxies)
        self.ns_pairs.extend(nameserver_pairs)

    def _get_nameservers(self, fqdn):
        for test, ns in self.ns_pairs:
            if test is None or test(fqdn): break
        else:
            ns = []
        return NameServers(
            self.client.port,
            nameservers=ns
        )

    @staticmethod
    def build_tester(rule) ->  Callable[
        [str],
        bool
    ]:

        if rule is None or callable(rule):
            return rule
        
        assert isinstance(rule, str)

        if rule.startswith('*.'):
            suffix = rule[1:]

            return lambda d: d.endswith(suffix)
        
        return lambda d: d == rule
    
    def add_entry(
        self,
        entry: DNSEntry
    ):
        record_type = self.types_map.types_by_name.get(entry.record_type)
        
        domain_targets = [
            str(target) for target in entry.domain_targets
        ]
        
        self.cache.add(
            entry.domain_name,
            record_type,
            domain_targets
        )

        nameserver_pairs = self.set_proxies(domain_targets)
        self.ns_pairs.extend(nameserver_pairs)

    def set_proxies(
        self, 
        proxies: List[Proxy]
    ):

        ns_pairs = []
        fallback = []

        if proxies:
            for item in proxies:

                if isinstance(item, str):
                    fallback.append(item)
                    continue

                test, nameserver = item
                if test is None:
                    fallback.extend(nameserver)
                    continue

                ns_pairs.append(
                    (
                        self.build_tester(test), 
                        NameServers(
                            self.client.port,
                            nameservers=nameserver
                        )
                    )
                )

        if fallback:
            ns_pairs.append(
                (
                    None, 
                    NameServers(
                        self.client.port,
                        nameservers=fallback
                    )
                )
            )

        return ns_pairs

    @memoizer.memoize_async(
            lambda _, fqdn, qtype, skip_cache: (fqdn, qtype)
    )
    async def _query(
        self, 
        fqdn: str, 
        qtype: RecordType,
        skip_cache: bool
    ):

        msg = DNSMessage()
        msg.qd.append(
            Record(
                REQUEST, 
                name=fqdn, 
                qtype=qtype
            )
        )

        has_result = False
        from_cache = False

        if skip_cache is False:
            has_result, fqdn = self.query_cache(msg, fqdn, qtype)
            from_cache = has_result

        while not has_result:
            nameservers = self._get_nameservers(fqdn)

            for addr in nameservers.iter():
                try:
                    res = await self.request(fqdn, qtype, addr)
                    assert res.ra, 'The upstream name server must be in recursive mode'
                    assert res.r != 2, 'Remote server failed'

                except:
                    nameservers.fail(addr)
                    raise

                else:
                    nameservers.success(addr)
                    self.cache_message(res)
                    msg.an.extend(res.an)
                    has_result = True
                    # has_result, fqdn = self.query_cache(msg, fqdn, qtype)
                    break

        return msg, from_cache

