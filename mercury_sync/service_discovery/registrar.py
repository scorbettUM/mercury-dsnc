import asyncio
import random
import time
from mercury_sync.env import (
    RegistrarEnv, 
    load_env
)
from mercury_sync.env.time_parser import TimeParser
from typing import Dict, List, Tuple, Union
from .dns import (
    DNSRegistrarService,
    DNSEntry
)

from .dns.core.nameservers.exceptions import NoNameServer
from .dns.core.record import Record
from .dns.core.record.record_data_types.a_record_data import ARecordData


class Registrar:

    def __init__(
        self,
        host: str,
        port: int,
        service_domain: str
    ) -> None:
        
        self.dns_service = DNSRegistrarService(
            host,
            port,
            service_domain
        )

        self._server_dns_entry = DNSEntry(
            domain_name=service_domain,
            record_type='A',
            domain_targets=(
                host,
            )
        )

        self.dns_service.server.add_entries([
            self._server_dns_entry
        ])

        env: RegistrarEnv = load_env(
            RegistrarEnv.types_map(),
            env_type=RegistrarEnv
        )

        self.expected_nodes = env.MERCURY_SYNC_REGISTRAR_EXPECTED_NODES
        self._max_poll_interval = TimeParser(env.MERCURY_SYNC_REGISTRAR_CLIENT_POLL_RATE).time
        self._min_poll_interval = self._max_poll_interval * 0.1
        self._poll_interval = random.uniform(
            self._min_poll_interval,
            self._max_poll_interval
        )

        self._timeout = TimeParser(env.MERCURY_SYNC_REGISTRATION_TIMEOUT).time
        self._registration_task: Union[asyncio.Task, None] = None

    async def run_registration(
        self,
        service_host: str,
        service_port: int
    ) -> List[DNSEntry]:

        await self.dns_service.start()

        discovered_nodes: Dict[Tuple[str], DNSEntry] = {
            self.dns_service.host: self._server_dns_entry
        }

        elapsed = 0
        start = time.monotonic()

        self._registration_task = asyncio.create_task(
            self.register(
                service_host,
                service_port
            )
        )

        while len(discovered_nodes) < self.expected_nodes and elapsed < self._timeout:

            current_registered = self.dns_service.gather_currently_registered()

            for node in current_registered:
                discovered_nodes.update({
                    str(node.domain_targets[0]): node
                })

            await asyncio.sleep(self._poll_interval)
            elapsed = time.monotonic() - start

            if self._registration_task.done():

                registration_results = await self._registration_task

                discovered_nodes.update({
                    str(entry.domain_targets[0]): entry for entry in registration_results
                })

        return list(discovered_nodes.values())
    

    async def run_forever(self):
        await self.dns_service.run_forever()

    async def register(
        self,
        service_host: str,
        service_port: int
    ):
        discovered_nodes: Dict[str, DNSEntry] = {}
        elapsed = 0
        start = time.monotonic()

        while len(discovered_nodes) < self.expected_nodes and elapsed < self._timeout:

            try:
                await self.dns_service.register(
                    service_host,
                    service_port
                )

            except ConnectionRefusedError:
                pass

            entries: List[DNSEntry] = []
            records: List[Record] = []

            try:
                response, _ = await self.dns_service.query(
                    self.dns_service.service_domain,
                    skip_cache=True
                )
                
                records = response.an

            except ConnectionRefusedError:
                pass

            except NoNameServer:
                pass

            except asyncio.TimeoutError:
                pass

            for record in records:

                record_data: ARecordData = record.data

                dns_entry = DNSEntry(
                    domain_name=record.name,
                    record_type=record_data.rtype.name,
                    domain_targets=(
                        record_data.data,
                    )
                )

                entries.append(dns_entry)

                if discovered_nodes.get(dns_entry.domain_targets) is None:

                    try:
                        new_node_records = await self.dns_service.query_known_nodes(
                            service_host,
                            service_port
                        )

                        entries.extend([
                            node_record for node_record in new_node_records.records
                        ])

                    except ConnectionRefusedError:
                        pass
            
            if len(entries) > 0:
                discovered_nodes.update({
                    str(entry.domain_targets[0]): entry for entry in entries
                })

            await asyncio.sleep(self._poll_interval)
            elapsed = time.monotonic() - start

        return list(discovered_nodes.values())

    async def close(self):
        await self.dns_service.close()
