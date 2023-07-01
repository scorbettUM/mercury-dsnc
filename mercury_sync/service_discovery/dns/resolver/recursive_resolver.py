import asyncio
import os
import pathlib
from urllib import request
from typing import Tuple, Optional


from mercury_sync.service_discovery.dns.core.address import Address
from mercury_sync.service_discovery.dns.core.dns_message import (
    DNSMessage,
    DNSError,
    REQUEST
)
from mercury_sync.service_discovery.dns.core.nameservers import NameServers
from mercury_sync.service_discovery.dns.core.record import (
    Record,
    RecordType, 
    RecordTypesMap
)
from mercury_sync.service_discovery.dns.core.record.record_data_types import (
    CNAMERecordData,
    SOARecordData,
    NSRecordData
)
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from .base_resolver import BaseResolver
from .memoizer import Memoizer



class RecursiveResolver(BaseResolver):
    '''Recursive DNS resolver.

    Resolve hostnames recursively from upstream name servers.
    '''
    name = 'RecursiveResolver'
    memoizer = Memoizer()

    def __init__(
        self, 
        host: str,
        port: int,
        *args, max_tick=5, 
        **kwargs
    ):
        super().__init__(
            host,
            port,
            *args, 
            **kwargs
        )

        self.max_tick = max_tick
        self.types_map = RecordTypesMap()
        
    def load_nameserver_cache(
        self,
        url: str='ftp://rs.internic.net/domain/named.cache',
        cache_file: str=os.path.join(
            os.getcwd(), 
            'named.cache.txt'
        ),
        timeout: Optional[int]=None
    ):
        
        if not os.path.isfile(cache_file):
            try:
                res = request.urlopen(
                    url, 
                    timeout=timeout
                )

                with open(cache_file, 'wb') as f:
                    f.write(res.read())

            except Exception:
                return

        cache_data = pathlib.Path(
            cache_file
        ).read_text().splitlines()

        for line in cache_data:
            if line.startswith(';'):
                continue
            parts = line.lower().split()
            if len(parts) < 4:
                continue

            name = parts[0].rstrip('.')
            # parts[1] (expires) is ignored
            qtype = self.types_map.types_by_name.get(
                parts[2], 
                RecordType.NONE
            )

            data_str = parts[3].rstrip('.')


            data = Record.create_rdata(
                qtype, 
                data_str
            )
            
            record = Record(
                name=name,
                qtype=qtype,
                data=data,
                ttl=-1,
            )

            self.cache.add(record=record)

    def add_entry(
        self,
        entry: DNSEntry
    ):
        record_type = self.types_map.types_by_name.get(entry.record_type)
        
        self.cache.add(
            entry.to_domain(),
            record_type,
            entry.to_data()
        )

    async def _query(
        self, 
        fqdn: str, 
        qtype: int,
        skip_cache: bool=False
    ):
        return await self._query_tick(
            fqdn, 
            qtype, 
            self.max_tick,
            skip_cache
        )

    def _get_nameservers(self, fqdn: str):
        '''Return a generator of parent domains'''

        hosts = []
        empty = True

        while fqdn and empty:
            if fqdn in ('in-addr.arpa', ):
                break
            _, _, fqdn = fqdn.partition('.')

            for rec in self.cache.query(fqdn, RecordType.NS):
                record_data: NSRecordData = rec.data
                host = record_data.data

                if Address.parse(
                    host, 
                    self.client.port,
                    allow_domain=True
                ).ip_type is None:
                    # host is a hostname instead of IP address

                    for res in self.cache.query(host, self.nameserver_types):
                        hosts.append(Address.parse(res.data.data))
                        empty = False

                else:
                    hosts.append(Address.parse(
                        host,
                        self.client.port
                    ))
                    empty = False

        return NameServers(
            self.client.port,
            nameservers=hosts
        )

    @memoizer.memoize_async(
        lambda _, fqdn, qtype, _tick, skip_cache: (fqdn, qtype)
    )
    async def _query_tick(
        self, 
        fqdn: str, 
        qtype: int,
        tick: int,
        skip_cache: bool
    ) -> Tuple[DNSMessage, bool]:
        msg = DNSMessage()
        msg.qd.append(Record(REQUEST, name=fqdn, qtype=qtype))

        has_result = False
        from_cache = False

        if skip_cache is False:
            has_result, fqdn = self.query_cache(msg, fqdn, qtype)
            from_cache = has_result

        last_err = None
        nameservers = self._get_nameservers(fqdn)

        while not has_result and tick > 0:

            tick -= 1

            for addr in nameservers.iter():
                try:
                    has_result, fqdn, nsips = await self._query_remote(
                        msg, fqdn, qtype, addr, tick)
                    nameservers = NameServers(
                        self.client.port,
                        nameservers=nsips
                    )

                except Exception as err:
                    last_err = err

                else:
                    break
            else:
                raise last_err or Exception('Unknown error')

        assert has_result, 'Maximum nested query times exceeded'

        return msg, from_cache

    async def _query_remote(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        qtype: RecordType,
        addr: Address, 
        tick: int
    ):
        
        res = await self.request(
            fqdn, 
            qtype, 
            addr
        )

        if res.qd[0].name != fqdn:
            raise DNSError(-1, 'Question section mismatch')
        
        assert res.r != 2, 'Remote server fail'

        self.cache_message(res)

        has_cname = False
        has_result = False
        has_ns = False

        for rec in res.an:
            msg.an.append(rec)

            if isinstance(rec.data, CNAMERecordData):
                fqdn = rec.data.data
                has_cname = True

            if rec.qtype != RecordType.CNAME or qtype in (
                RecordType.ANY, 
                RecordType.CNAME
            ):
                has_result = True

        for rec in res.ns:
            if rec.qtype in (
                RecordType.NS, 
                RecordType.SOA
            ):
                has_result = True

            else:
                has_ns = True

        if not has_cname and not has_ns:
            # Not found, return server fail since we are not authorative
            msg.r = 2
            has_result = True
        if has_result:
            return has_result, fqdn, []

        # Load name server IPs from res.ar
        namespace_ip_address_map = {}

        for rec in res.ar:
            if rec.qtype in self.nameserver_types:
                namespace_ip_address_map[rec.name, rec.qtype] = rec.data.data

        hosts = []
        for rec in res.ns:

            if isinstance(rec.data, SOARecordData):
                hosts.append(rec.data.mname)

            elif isinstance(rec.data, NSRecordData):
                hosts.append(rec.data.data)

        namespace_ips = []

        for host in hosts:
            for t in self.nameserver_types:
                ip = namespace_ip_address_map.get((host, t))

                if ip is not None:
                    namespace_ips.append(ip)

        # Usually name server IPs will be included in res.ar.
        # In case they are not, query from remote.
        if len(namespace_ips) < 1 and len(hosts) > 0:

            tick -= 1

            for t in self.nameserver_types:
                for host in hosts:
                    try:
                        ns_res, _ = await asyncio.shield(
                            self._query_tick(host, t, tick)
                        )

                    except Exception:
                        pass

                    else:
                        for rec in ns_res.an:
                            if rec.qtype == t:
                                namespace_ips.append(rec.data.data)
                                break

                if len(namespace_ips) > 0:
                    break

        return has_result, fqdn, namespace_ips

