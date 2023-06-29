import asyncio
from typing import List, Tuple, Union
from mercury_sync.service_discovery.dns.core.address import (
    Address,
    InvalidHost,
    InvalidIP

)
from mercury_sync.service_discovery.dns.core.cache import CacheNode
from mercury_sync.service_discovery.dns.core.dns_message import (
    DNSMessage,
    DNSError
)
from mercury_sync.service_discovery.dns.core.record import RecordType
from mercury_sync.service_discovery.dns.core.record.record_data_types import (
    CNAMERecordData,
    NSRecordData
)
from mercury_sync.service_discovery.dns.request.client import DNSClient


class BaseResolver:
    zone_domains = []
    nameserver_types = [
        RecordType.A
    ]

    def __init__(    
        self,
        host: str,
        port: int,
        cache: CacheNode = None,
        query_timeout: float = 3.0,
        request_timeout: float = 5.0
    ):
        self._queries = {}
        self.cache = cache or CacheNode()
        self.request_timeout = request_timeout
        self.query_timeout = query_timeout
        self.client = DNSClient(
            host,
            port,
            query_callback=self.query
        )

    def cache_message(self, msg: DNSMessage):
        for rec in msg.an + msg.ns + msg.ar:
            if rec.ttl > 0 and rec.qtype != RecordType.SOA:
                self.cache.add(record=rec)

    def set_zone_domains(self, domains: List[str]):
        self.zone_domains = [
            domain.lstrip('.') for domain in domains
        ]

    async def _query(self, _fqdn: str, _qtype: RecordType) -> Tuple[DNSMessage, bool]:
        raise NotImplementedError

    async def query(
        self,
        fqdn: str,
        qtype: RecordType=RecordType.ANY
    ) -> Tuple[DNSMessage, bool]:
        
        if fqdn.endswith('.'):
            fqdn = fqdn[:-1]

        if qtype == RecordType.ANY:
            try:
                addr = Address.parse(
                    fqdn,
                    self.client.port
                )
                ptr_name = addr.to_ptr()

            except (InvalidHost, InvalidIP):
                pass

            else:
                fqdn = ptr_name
                qtype = RecordType.PTR

        return await asyncio.wait_for(
            self._query(
                fqdn, 
                qtype
            ),
            timeout=self.query_timeout
        )

    async def request(
        self, 
        fqdn: str, 
        qtype: RecordType, 
        addr: Address
    ):

        result = await self.client.query(fqdn, qtype, addr)
        if result.qd[0].name != fqdn:
            raise DNSError(-1, 'Question section mismatch')
        
        assert result.r != 2, 'Remote server fail'

        self.cache_message(result)

        return result

    def _add_cache_cname(self, msg: DNSMessage, fqdn: str) -> Union[str, None]:
        '''Query cache for CNAME records and add to result msg.
        '''
        for cname in self.cache.query(fqdn, RecordType.CNAME):
            msg.an.append(cname.copy(name=fqdn))
            if isinstance(cname.data, CNAMERecordData):
                return cname.data.data

    def _add_cache_qtype(
        self, 
        msg: DNSMessage, 
        fqdn: str, 
        qtype: RecordType
    ) -> bool:
        '''Query cache for records other than CNAME and add to result msg.
        '''
        if qtype == RecordType.CNAME:
            return False
        
        has_result = False
        for rec in self.cache.query(fqdn, qtype):
            if isinstance(rec.data, NSRecordData):
                a_res = list(self.cache.query(
                    rec.data.data, (
                        RecordType.A,
                        RecordType.AAAA
                    )
                ))
                
                if a_res:
                    msg.ar.extend(a_res)
                    msg.ns.append(rec)
                    has_result = True
            else:
                msg.an.append(rec.copy(name=fqdn))
                has_result = True

        return has_result

    def query_cache(
        self, 
        msg: DNSMessage,
        fqdn: str, 
        qtype: RecordType
    ):

        cnames = set()

        while True:
            cname = self._add_cache_cname(msg, fqdn)
            if not cname: break
            if cname in cnames:
                # CNAME cycle detected
                break
            cnames.add(cname)
            # RFC1034: If a CNAME RR is present at a node, no other data should be present
            fqdn = cname
        has_result = bool(cname) and qtype in (RecordType.CNAME, RecordType.ANY)

        if qtype != RecordType.CNAME:
            has_result = self._add_cache_qtype(msg, fqdn, qtype) or has_result

        if any(('.' + fqdn).endswith(root) for root in self.zone_domains):
            if not has_result:
                msg.r = 3
                has_result = True
            msg.aa = 1
        # fqdn may change due to CNAME
        return has_result, fqdn
