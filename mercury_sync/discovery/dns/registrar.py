import socket
from mercury_sync.discovery.dns.core.url import URL
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

from mercury_sync.discovery.dns.resolver import DNSResolver
from mercury_sync.types import Call
from typing import (
    Optional, 
    List,
    Union,
    Dict,
    Tuple
)


class Registrar(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        env: Env=None
    ) -> None:

        if env is None:
            env = load_env(Env)
    
        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            env=env,
            engine='async'
        )

        self.resolver = DNSResolver(
            host,
            port,
            self._instance_id,
            self._env
        )

        self.random_id_generator = RandomIDGenerator()

        self._nameservers: List[URL] = []
        self._next_nameserver_idx = 0
        self._connected_namservers: Dict[Tuple[str, int], bool] = {}
        self._connected_domains: Dict[str, bool] = {}

    def add_entries(
        self,
        entries: List[DNSEntry]
    ):

        for entry in entries:
            for domain, record in entry.to_record_data():
                
                self.resolver.add_to_cache(
                    domain,
                    record.rtype,
                    record
                )

    async def add_nameservers(
        self,
        urls: List[str]
    ):
        urls = self.resolver.add_nameservers(urls)

        await self.resolver.connect_nameservers(
            urls,
            cert_path=self.cert_path,
            key_path=self.key_path
        )

        self._nameservers.extend(urls)

    def _next_nameserver_url(self) -> Union[URL, None]:

        if len(self._nameservers) > 0:

            namserver_url = self._nameservers[self._next_nameserver_idx]

            self._next_nameserver_idx = (
                self._next_nameserver_idx + 1
            )%len(self._nameservers)

            return namserver_url



    @server()
    async def resolve_query(
        self,
        shard_id: int,
        query: DNSMessage
    ) -> Call[DNSMessage]:
        
        messages: List[DNSMessage] = []
        
        for record in query.query_domains:
                  
            dns_message, has_result = await self.resolver.query(
                record.name,
                record_type=record.record_type
            )

            if has_result is False:
                # TODO: Query using client.
                pass

            dns_data = dns_message.to_data()
            dns_data.update({
                'query_id': query.query_id,
                'has_result': has_result
            })

            response = DNSMessage(**dns_data)

            messages.append(response)

        return DNSMessageGroup(
            messages=messages
        )
            
    @client('resolve_query')
    async def submit_query(
        self,
        host: str,
        port: int,
        entry: DNSEntry
    ) -> Call[DNSMessageGroup]:
   
        return DNSMessage(
            host=host,
            port=port,
            query_domains=[
                Record(
                    name=domain,
                    record_type=record.rtype,
                    data=record,
                    ttl=entry.time_to_live

                ) for domain, record in entry.to_record_data()
            ]
        )
    
    async def query(
        self,
        entry: DNSEntry
    ):

        nameserver_url = self._next_nameserver_url()

        host = nameserver_url.host
        port = nameserver_url.port

        if nameserver_url.ip_type is not None:
            host = socket.gethostbyname(nameserver_url.host)

        if not self._connected_namservers.get((host, port)):
            
            await self.start_client(
                DNSMessage(
                    host=host,
                    port=port
                )
            )

            self._connected_namservers[(host, port)] = True

        return await self.submit_query(
            host,
            port,
            entry
        )



        

