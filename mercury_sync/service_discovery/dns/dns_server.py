
import socket
from mercury_sync.env import Env
from typing import Optional
from .server import (
    TCPServer,
    UDPServer
)

class DNSResolver:

    def __init__(
        self,
        host: str,
        port: int,
        dns_port: int,
        instance_id: int,
        env: Env
    ) -> None:
        
        self.udp_dns_server = UDPServer(
            host,
            port,
            dns_port,
            instance_id,
            env
        )

        self.tcp_dns_server = TCPServer(
            host,
            port + 1,
            dns_port + 1,
            instance_id,
            env
        )

    async def start_server(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ):
        await self.udp_dns_server.start_dns_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket
        )

        await self.tcp_dns_server.start_dns_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket
        )

    async def close(self):
        await self.udp_dns_server.close()
        await self.tcp_dns_server.close()