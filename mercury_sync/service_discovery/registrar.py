from .dns import (
    DNSService,
    DNSEntry
)


class Registrar:

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

        self.dns_service.server.add_entries([
            DNSEntry(
                domain_name=service_domain,
                record_type='A',
                domain_targets=(
                    host,
                )
            )
        ])

    async def start(self):
        await self.dns_service.start()
        
    async def run_forever(self):
        await self.dns_service.run_forever()

    async def close(self):
        await self.dns_service.close()
