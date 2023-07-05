from mercury_sync.env import load_env, Env
from mercury_sync.hooks import (
    client,
    server
)
from mercury_sync.models.dns_message import DNSMessage
from mercury_sync.models.dns_message_group import DNSMessageGroup
from mercury_sync.service.controller import Controller
from mercury_sync.discovery.dns.core.record import (
    DNSEntry,
    Record
)
from mercury_sync.discovery.dns.core.random import RandomIDGenerator

from mercury_sync.discovery.dns.resolver.cache_resolver import CacheResolver
from mercury_sync.types import Call
from typing import (
    Optional, 
    List
)


class Registrar(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> None:

        env = load_env(Env.types_map())
        self.resolver = CacheResolver(
            port
        )
    
        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            env=env,
            engine='async'
        )

        self.random_id_generator = RandomIDGenerator()

    def add_entries(
        self,
        entries: List[DNSEntry]
    ):

        for entry in entries:
            for domain, record in entry.to_record_data():
                
                self.resolver.cache.add(
                    fqdn=domain,
                    record_type=record.rtype,
                    data=record
                )

    @server()
    async def resolve_query(
        self,
        shard_id: int,
        query: DNSMessage
    ) -> Call[DNSMessage]:
        
        messages: List[DNSMessage] = []
        
        for record in query.query_domains:
                  
            dns_query, has_result = self.resolver.query_cache(
                record.name,
                record.record_type
            )

            if has_result is False:
                # TODO: Query using client.
                pass

            response = DNSMessage(
                **dns_query.to_data(),
                query_id=query.query_id,
                has_result=has_result
            )

            messages.append(response)

        return DNSMessageGroup(
            messages=messages
        )
            
    @client('resolve_query')
    async def query(
        self,
        host: str,
        port: int,
        records: List[Record]
    ) -> Call[DNSMessage]:
   
        return DNSMessage(
            host=host,
            port=port,
            query_domains=records
        )



        

