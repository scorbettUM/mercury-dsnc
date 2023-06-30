import asyncio
import time
from mercury_sync.connection.addresses.subnet_range import SubnetRange
from mercury_sync.env import (
    RegistrarEnv, 
    load_env
)
from mercury_sync.env.time_parser import TimeParser
from typing import Dict
from .dns import (
    DNSService,
    DNSEntry
)
from .dns.core.record.record_data_types.a_record_data import ARecordData

class RegistrarClient:

    def __init__(
        self,
        host: str,
        port: int,
        service_domain: str
    ) -> None:
        self.dns_service = DNSService(
            host,
            port,
            service_domain
        )


        env: RegistrarEnv = load_env(
            RegistrarEnv.types_map(),
            env_type=RegistrarEnv
        )

        self.expected_nodes = env.MERCURY_SYNC_REGISTRAR_EXPECTED_NODES
        self._poll_interval = TimeParser(env.MERCURY_SYNC_REGISTRAR_CLIENT_POLL_RATE).time
        self._timeout = TimeParser(env.MERCURY_SYNC_REGISTRATION_TIMEOUT).time

    async def publish(
        self,
        dns_service_host: str,
        dns_service_port: int
    ):
        return await self.dns_service.register(
            dns_service_host,
            dns_service_port
        )
    
    async def poll(
        self
    ):
        discovered_nodes: Dict[str, DNSEntry] = {}
        elapsed = 0
        start = time.monotonic()

        while len(discovered_nodes) < self.expected_nodes and elapsed < self._timeout:

            response, _ = await self.dns_service.query(
                self.dns_service.service_domain,
                skip_cache=True
            )
            
            records = response.an

            for record in records:

                record_data: ARecordData = record.data
                a_record = record_data

                

                discovered_nodes[a_record.data] = DNSEntry(
                    domain_name=record.name,
                    record_type=a_record.rtype.name,
                    domain_targets=(
                        a_record.data,
                    )
                )

                await asyncio.sleep(self._poll_interval)
                elapsed = time.monotonic() - start

        return list(discovered_nodes.values())



