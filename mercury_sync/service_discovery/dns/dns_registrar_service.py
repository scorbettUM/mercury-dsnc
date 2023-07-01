import asyncio
import traceback
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
        self.registered_clients: Dict[Tuple[str, str], DNSEntry] = {}

        self._default_nameservers: List[DNSEntry] = [
            DNSEntry(
                domain_types=["_udp"],
                domain_name=f'{self.service_domain}.dns',
                domain_port=self.dns_port,
                domain_targets=(
                    self.host,
                ),
                record_type='SRV',
            ),
            DNSEntry(
                domain_types=["_tcp"],
                domain_name=f'{self.service_domain}.dns',
                domain_port=self.dns_port + 1,
                domain_targets=(
                    self.host,
                ),
                record_type='SRV',
            )
        ]

        self.default_records: List[DNSEntry] = [
            DNSEntry(
                domain_types=["_udp"],
                domain_name=self.service_domain,
                domain_port=self.port,
                domain_targets=(
                    self.host,
                ),
                record_type='SRV',
            ),
            DNSEntry(
                domain_types=["_tcp"],
                domain_name=self.service_domain,
                domain_port=self.port + 1,
                domain_targets=(
                    self.host,
                ),
                record_type='SRV',
            ),
            *self._default_nameservers
        ]

    async def start(self):
        await self.start_server()
        await self.server.start()

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def query(
        self,
        domain_name: str,
        record_type: Literal[
            "A", 
            "AAAA", 
            "CNAME", 
            "SRV", 
            "TXT"
        ]="SRV",
        domain_types: List[
            Literal[
                "_udp", 
                "_tcp", 
                "_tcps", 
                "_http", 
                "_https"
            ]
        ]=["_udp"],
        skip_cache: bool=False
    ):
        
        if record_type == "SRV":
            domain_types = '.'.join(domain_types)
            domain_name = f'{domain_types}.{domain_name}'

        return await self.resolver.query(
            domain_name,
            record_type,
            skip_cache=skip_cache
        )
    
    async def register(
        self,
        host: str,
        port: int,
        record_type: Literal[
            "A", 
            "AAAA", 
            "CNAME", 
            "SRV", 
            "TXT"
        ]="SRV",
        domain_types: List[
            Literal[
                "_udp", 
                "_tcp", 
                "_tcps", 
                "_http", 
                "_https"
            ]
        ]=["_udp"]
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
            record_type,
            domain_types=domain_types
        )

        for record in registration.records:
            domain_target = str(record.domain_targets[0])
            self.registered_clients[(record.record_type, domain_target)] = record

        self.server.add_entries(registration.records)
        self.resolver.add_entries(registration.records)

        return registration
    
    async def query_known_nodes(
        self,
        host: str,
        port: int,
        record_type: Literal[
            "A", 
            "AAAA", 
            "CNAME", 
            "SRV", 
            "TXT"
        ]="SRV",
        domain_types: List[
            Literal[
                "_udp", 
                "_tcp", 
                "_tcps", 
                "_http", 
                "_https"
            ]
        ]=["_udp"]
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
            record_type,
            domain_types=domain_types
        )

        for record in registration.records:
            domain_target = str(record.domain_targets[0])
            self.registered_clients[(record.record_type, domain_target)] = record

        self.server.add_entries(registration.records)
        self.resolver.add_entries(registration.records)

        return registration

    @server()
    async def register_service(
        self,
        shard_id: int,
        registration: Registration
    ) -> Call[Registration]:
        
        nameservers: List[DNSEntry] = self._default_nameservers
    
        self.server.add_entries(
            registration.records
        )

        self.resolver.add_entries(
            registration.records
        )

        
        for record in registration.records:
            domain_target = str(record.domain_targets[0])
            self.registered_clients[(record.record_type, domain_target)] = record

            entry_nameservers = self.server.get_nameserver_addresses(
                record.domain_name
            )

            nameservers.extend([
                DNSEntry(
                    domain_types=[
                        address.domain_type
                    ],
                    domain_name=record.domain_name,
                    domain_port=address.hostinfo.port,
                    record_type=record.record_type,
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

        for record in registered.records:
            domain_target = str(record.domain_targets[0])
            self.registered_clients[(record.record_type, domain_target)] = record

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
        record_type: Literal[
            "A", 
            "AAAA", 
            "CNAME", 
            "SRV", 
            "TXT"
        ]="SRV",
        domain_types: List[
            Literal[
                "_udp", 
                "_tcp", 
                "_tcps", 
                "_http", 
                "_https"
            ]
        ]=["_udp"],
        domain_values: Dict[str, str]={}
    ) -> Call[Registration]:
        

        return Registration(
            host=host,
            port=port,
            records=[
                DNSEntry(
                    domain_types=domain_types,
                    domain_name=self.service_domain,
                    domain_port=self.port,
                    domain_targets=(
                        self.host,
                    ),
                    record_type=record_type,
                    domain_values=domain_values
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
        return list(self.registered_clients.values())
    
    async def shutdown(self):
        await self.close()
        await self.server.close()

        