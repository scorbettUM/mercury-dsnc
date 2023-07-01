import asyncio
from collections import defaultdict
from mercury_sync.env import load_env, Env
from mercury_sync.hooks import (
    client,
    server
)
from mercury_sync.models.registration import Registration
from mercury_sync.service.controller import Controller
from mercury_sync.types import Call
from typing import (
    Union, 
    Optional, 
    List, 
    Literal, 
    Dict, 
    Tuple
)
from .resolver import DNSResolver
from .server import (
    DNSServer,
    DNSEntry
)


class DNSRegistrarService(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        service_domain: str,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        entries: List[DNSEntry]=[],
        resolver: Literal["proxy", "recursive"]="proxy"
    ) -> None:

        env = load_env(Env.types_map())
    
        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            env=env,
            engine='async'
        )

        proxy_servers: List[str] = []
        for entry in entries:
            for target in entry.domain_targets:
                proxy_servers.append(target)

        self.dns_port = port + 2
        self.service_domain = service_domain
        
        self.server = DNSServer(
            host,
            self.dns_port,
            cert_path=cert_path,
            key_path=key_path,
            env=env,
            entries=entries,
            proxy_servers=proxy_servers,
        )

        self._waiter: Union[asyncio.Future, None] = None

        self.resolver = DNSResolver(
            host,
            self.dns_port,
            resolver=resolver,
            proxies=proxy_servers
        )

        self.resolver.add_entries(
            entries
        )

        self._known_nodes: Dict[Tuple[str, int], bool] = {}
        self.registered_clients: Dict[str, Tuple[str, Tuple[str]]] = {}

    async def start(self):
        await self.start_server()
        await self.server.start()

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def query(
        self,
        domain_name: str,
        record_type: Literal["A", "AAAA", "CNAME", "TXT"]="A",
        skip_cache: bool=False
    ):
        return await self.resolver.query(
            domain_name,
            record_type,
            skip_cache=skip_cache
        )
    
    async def register(
        self,
        host: str,
        port: int,
        record_type: Literal["A", "AAAA", "CNAME", "TXT"]="A"
    ):
        known_nodes_count = len(self._known_nodes)
        known_node = self._known_nodes.get((host, port))

        if known_node is None and known_nodes_count < 1:
            await self.start_client(
                Registration(
                    host=host,
                    port=port,
                    records=[]
                )
            )

            self._known_nodes[(host, port)] = True

        elif known_node is None:
            await self.extend_client(
                Registration(
                    host=host,
                    port=port,
                    records=[]
                )
            )
        
        _, registration = await self.push_registration(
            host,
            port,
            record_type
        )

        for record in registration.records:
            self.registered_clients[record.domain_targets] = (
                record.record_type,
                record.domain_name
            )

        self.server.add_entries(registration.records)
        self.resolver.add_entries(registration.records)

        return registration
    
    async def query_known_nodes(
        self,
        host: str,
        port: int,
        record_type: Literal["A", "AAAA", "CNAME", "TXT"]="A"
    ):
        known_nodes_count = len(self._known_nodes)
        known_node = self._known_nodes.get((host, port))

        if known_node is None and known_nodes_count < 1:
            await self.start_client(
                Registration(
                    host=host,
                    port=port,
                    records=[]
                )
            )

            self._known_nodes[(host, port)] = True

        elif known_node is None:
            await self.extend_client(
                Registration(
                    host=host,
                    port=port,
                    records=[]
                )
            )

        _, registration = await self.push_registration(
            host,
            port,
            record_type
        )

        for record in registration.records:
            self.registered_clients[record.domain_targets] = (
                record.record_type,
                record.domain_name
            )

        self.server.add_entries(registration.records)
        self.resolver.add_entries(registration.records)

        return registration

    @server()
    async def register_service(
        self,
        shard_id: int,
        registration: Registration
    ) -> Call[Registration]:
        
        self.server.add_entries(
            registration.records
        )

        self.resolver.add_entries(
            registration.records
        )

        nameservers: List[DNSEntry] = [
            DNSEntry(
                    domain_name=self.service_domain,
                    record_type='A',
                    domain_targets=(
                        self.host,
                    )
                )
        ]

        for entry in registration.records:

            self.registered_clients[entry.domain_targets] = (
                entry.record_type,
                entry.domain_name
            )

            entry_nameservers = self.server.get_nameserver_addresses(
                entry.domain_name
            )

            nameservers.extend([
                DNSEntry(
                    domain_name=entry.domain_name,
                    record_type=entry.record_type,
                    domain_targets=(
                        address.hostinfo.hostname,
                    )
                ) for address in entry_nameservers
            ])

        return Registration(
            host=self.host,
            port=self.dns_port,
            records=nameservers
        )
    
    @server()
    async def update_registered(
        self,
        shard_id: int,
        registered: Registration
    ) -> Call[Registration]:
        
        current_records = self.gather_currently_registered()

        for entry in registered.records:
            self.registered_clients[entry.domain_targets] = (
                entry.record_type,
                entry.domain_name
            )

        return Registration(
            host=self.host,
            port=self.port,
            records=current_records
        )
    
    @client('register_service')
    async def push_registration(
        self,
        host: str,
        port: int,
        record_type: str
    ) -> Call[Registration]:
        return Registration(
            host=host,
            port=port,
            records=[
                DNSEntry(
                    domain_name=self.service_domain,
                    record_type=record_type,
                    domain_targets=(
                        self.host,
                    )
                )
            ]
        )
    
    @client('update_registered')
    async def push_registered_query(
        self,
        host: str,
        port: int
    ) -> Call[Registration]:
        
        current_records = self.gather_currently_registered()

        return Registration(
            host=host,
            port=port,
            records=current_records
        )
    

    def gather_currently_registered(self) -> List[DNSEntry]:

        currently_registered: List[DNSEntry] = []

        for domain_targets, domain_data in self.registered_clients.items():

            record_type, domain_name = domain_data

            currently_registered.append(
                DNSEntry(
                    domain_name=domain_name,
                    record_type=record_type,
                    domain_targets=domain_targets
                )
            )

        return currently_registered
    
    async def shutdown(self):
        await self.close()
        await self.server.close()

        