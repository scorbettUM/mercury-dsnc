import asyncio
import random
from mercury_sync.env import load_env, Env
from mercury_sync.models.message import Message
from mercury_sync.service_discovery.dns.server.entries import DNSEntry
from typing import Union, Optional, Dict, List
from .tcp_server import TCPServer
from .udp_server import UDPServer


class DNSServer:

    def __init__(
        self,
        host: str,
        dns_port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        env: Optional[Env]=None,
        entries: List[DNSEntry]=[],
        proxy_servers: Optional[List[str]]=None
    ) -> None:

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._instance_id = random.randint(0, 2**16)
        self._response_parsers: Dict[str, Message] = {}

        self.host = host
        self.port = dns_port
        self.dns_port = dns_port
        self.cert_path = cert_path
        self.key_path = key_path

        if env is None:
            env = load_env(Env.types_map())

        self._udp_server = UDPServer(
            host,
            dns_port,
            self._instance_id,
            entries=entries,
            env=env,
            proxy_servers=proxy_servers 
        )

        self._tcp_server = TCPServer(
            host,
            dns_port + 1,
            self._instance_id,
            entries=entries,
            env=env,
            proxy_servers=proxy_servers 
        )

        self._waiter: Union[asyncio.Future, None] = None

    def add_entries(
        self,
        entries: List[DNSEntry]
    ):
        for entry in entries:
            self._udp_server.handler.add_entry(entry)
            self._tcp_server.handler.add_entry(entry)

    def get_nameserver_addresses(
        self,
        domain_name: str
    ):
        udp_nameserver_addresses = self._udp_server.handler.get_nameserver_addresses(
            domain_name
        )

        tcp_nameserver_addresses = self._tcp_server.handler.get_nameserver_addresses(
            domain_name
        )

        return [
            *udp_nameserver_addresses,
            *tcp_nameserver_addresses
        ]


    async def start(self):
        await self._udp_server.start_dns_server(
            cert_path=self.cert_path,
            key_path=self.key_path
        )

        await self._tcp_server.start_dns_server(
            cert_path=self.cert_path,
            key_path=self.key_path
        )

    async def run_forever(self):
        loop = asyncio.get_event_loop()
        self._waiter = loop.create_future()
        await self._waiter

    async def close(self):
        await self._udp_server.close()
        await self._tcp_server.close()

        self._waiter.set_result(None)
        
