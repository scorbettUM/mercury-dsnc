import asyncio
import traceback
from mercury_sync.env import load_env, Env
from mercury_sync.hooks import (
    client,
    server
)
from mercury_sync.models.registration import Registration
from mercury_sync.service.controller import Controller
from mercury_sync.types import Call
from typing import Union, Optional, List, Literal, Dict, Tuple
from .resolver import DNSResolver
from .server import (
    DNSServer,
    DNSEntry
)


class DNSService(Controller):

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

        self._registered_nameservers: Dict[Tuple[str, int], bool] = {}

    async def start(self):
        await self.start_server()
        await self.server.start()

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def query(
        self,
        domain_name: str,
        record_type: Literal["A", "AAAA", "CNAME", "TXT"]="A"
    ):
        return await self.resolver.query(
            domain_name,
            record_type
        )
    
    async def register(
        self,
        host: str,
        port: int,
        record_type: Literal["A", "AAAA", "CNAME", "TXT"]="A"
    ):
        if self._registered_nameservers.get((host, port)) is None:
            await self.start_client(
                Registration(
                    host=host,
                    port=port,
                    records=[]
                )
            )

            self._registered_nameservers[(host, port)] = True
        
        _, registration = await self.push_registration(
            host,
            port,
            record_type
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
    
    async def shutdown(self):
        await self.close()
        await self.server.close()

        