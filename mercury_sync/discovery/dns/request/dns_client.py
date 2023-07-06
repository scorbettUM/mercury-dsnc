import socket
from mercury_sync.connection.base.connection_type import ConnectionType
from mercury_sync.connection.tcp import (
    MercurySyncHTTPConnection,
    MercurySyncTCPConnection
)
from mercury_sync.connection.udp import MercurySyncUDPConnection
from mercury_sync.env import Env, RegistrarEnv, load_env
from mercury_sync.models.dns_message import DNSMessage
from mercury_sync.models.http_message import HTTPMessage
from mercury_sync.discovery.dns.core.url import URL
from typing import Optional, Tuple, Union, Dict


class DNSClient:

    def __init__(
        self,
        host: str,
        port: int,
        instance_id: str,
        env: Env
    ) -> None:
        
        registrar_env: RegistrarEnv = load_env(RegistrarEnv)
        
        self.host = host
        self.port = port
        self.instance_id = instance_id
        self.env = env

        self._client_config = (
            host,
            port,
            instance_id,
            env
        )
        
        self._connection_types: Dict[
            ConnectionType,
            Union[
                MercurySyncUDPConnection,
                MercurySyncTCPConnection,
                MercurySyncHTTPConnection
            ]
        ] = {
            ConnectionType.UDP: lambda config: MercurySyncUDPConnection(*config),
            ConnectionType.TCP: lambda config: MercurySyncTCPConnection(*config),
            ConnectionType.HTTP: lambda config: MercurySyncHTTPConnection(*config),
        }

        self._client: Union[
            MercurySyncUDPConnection,
            MercurySyncTCPConnection,
            MercurySyncHTTPConnection,
            None
        ] = None

        self._client_types = {
            "udp": ConnectionType.UDP,
            "tcp": ConnectionType.TCP,
            "http": ConnectionType.HTTP
        }

        self.client_type = self._client_types.get(
            registrar_env.MERCURY_SYNC_RESOLVER_CONNECTION_TYPE
        )

        self._connections: Dict[Tuple[str, int], bool] = {}
        self.cert_paths: Dict[str, str] = {}
        self.key_paths: Dict[str, str] = {}

    async def connect_client(
        self,
        url: URL,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ):
        
        self.cert_paths[url.address] = cert_path
        self.key_paths[url.address] = key_path
        
        self._client: Union[
            MercurySyncUDPConnection,
            MercurySyncTCPConnection,
            MercurySyncHTTPConnection
        ] = self._connection_types.get(
            self.client_type
        )(self._client_config)

        if self._client.connection_type == ConnectionType.TCP:
            await self._client.connect_client(
                url.address,
                cert_path=cert_path,
                key_path=key_path,
                worker_socket=worker_socket
            )

        elif self._client.connection_type == ConnectionType.HTTP:
            await self._client.connect_client(
                url.address,
                is_ssl=url.is_ssl,
                hostname=url.host,
                worker_socket=worker_socket
            )

        else:
            await self._client.connect_async(
                cert_path=cert_path,
                key_path=key_path,
                worker_socket=worker_socket
            )

    async def send(
        self,
        event_name: str,
        data: DNSMessage,
        url: URL
    ):
        
        if self._client is None:
            await self.connect_client(
                url
            )
        
        if self._client.connection_type == ConnectionType.TCP:
            response = await self._client.send_bytes(
                event_name,
                data.to_tcp_bytes(),
                url.address
            )

            return DNSMessage.parse(response)
        
        elif self._client.connection_type == ConnectionType.HTTP:
            response: HTTPMessage = await self._client.send(
                event_name,
                data.to_http_bytes(url.url),
                url.address
            )

            return DNSMessage.parse(response.data)
        
        else:

            response = await self._client.send_bytes(
                event_name,
                data.to_udp_bytes(),
                url.address
            )

            return DNSMessage.parse(response)
            