import asyncio


from mercury_sync.service_discovery.dns.core.address import (
    Address
)
from mercury_sync.service_discovery.dns.core.dns_message import (
    DNSMessage,
    REQUEST
)
from mercury_sync.service_discovery.dns.core.record import (
    Record,
    RecordType,
    RecordTypesMap
)

from typing import (
    Dict, 
    Tuple, 
    Union, 
    Literal, 
    Callable,
    Optional,
    Coroutine,
    Any
)

from .http.http_client import HTTPClient
from .tcp.tcp_client import TCPClient
from .udp.udp_client import UDPClient


class DNSClient:
    '''
    Resolve a name by requesting a remote name server.
    '''


    def __init__(
        self,
        host: str,
        port: int,
        query_callback: Optional[
            Callable[
                [str],
                Coroutine[
                    Any,
                    Any,
                    Tuple[DNSMessage, bool]
                ]
            ]
        ]=None
    ):
        self.request_cache: Dict[Tuple[str, RecordType, Address], asyncio.Task] = {}
        self.types_map = RecordTypesMap()
        self.query_callback = query_callback
        self.host = host
        self.port = port

        self._clients: Dict[
            Literal[
                "tcp",
                "tcps",
                "udp",
                "https"
            ],
            Callable[
                [RecordType],
                Union[
                    HTTPClient,
                    TCPClient,
                    UDPClient
                ]
            ]
        ] = {
            'http': lambda record_type: HTTPClient(
                record_type,
                self.host,
                self.port,
                query_callback=self.query_callback
            ),
            'https': lambda record_type: HTTPClient(
                record_type,
                self.host,
                self.port,
                query_callback=self.query_callback
            ),
            'tcp': lambda record_type: TCPClient(  
                record_type,
                self.host,
                self.port,
                query_callback=self.query_callback
            ),
            'tcps': lambda record_type: TCPClient(
                record_type,
                self.host,
                self.port,
                query_callback=self.query_callback
            ),
            'udp': lambda record_type: UDPClient(
                record_type,
                self.host,
                self.port,
                query_callback=self.query_callback
            )
        }

    async def query(
        self, 
        fqdn: str, 
        qtype: RecordType, 
        address: Address,
        timeout: Optional[int]=None
    ):
        dns_request = DNSMessage(qr=REQUEST)
        dns_request.qd.append(Record(REQUEST, fqdn, qtype))

        client = self._clients.get(
            address.protocol,
            HTTPClient(
                self.host,
                self.port,
                qtype
            )
        )(
            qtype
        )


        return await asyncio.wait_for(
            client.query(dns_request, address), 
            timeout=timeout        )
