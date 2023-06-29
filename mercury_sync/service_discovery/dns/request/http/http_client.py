import base64
import json
import urllib.parse
from urllib.parse import ParseResult
from mercury_sync.service_discovery.dns.core.dns_message import DNSMessage, REQUEST
from mercury_sync.service_discovery.dns.core.record import (
    Record, 
    RecordType, 
    RecordTypesMap
)
from mercury_sync.service_discovery.dns.request.connections.connection_handler import (
    ConnectionHandler
)
from typing import (
    Union, 
    Dict, 
    Optional, 
    Literal,
    Callable,
    Tuple,
    Any,
    Coroutine
)
from .http_response import HTTPResponse



class HTTPClient:
    session = None

    def __init__(
        self,
        record_type: RecordType,
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
        self.record_type = record_type
        self.host = host
        self.port = port
        self.query_callback = query_callback

        self.types_map = RecordTypesMap()
    
    async def send_request(
        self,
        url: str,
        method: Literal["GET", "POST"]='GET',
        params: Optional[Dict[str, str]]=None,
        headers: Dict[str, str]=None,
        data: Optional[str]=None
    ):

        if '://' not in url:
            url = 'http://' + url      

        if params and "?" in url:
            params = f'{url}&'

        elif params:
            url = f'{url}?'

        if params:
            query_string = urllib.parse.urlencode(params)
            url = f'{url}{query_string}'

        parsed_result: ParseResult = urllib.parse.urlparse(url)

        path = parsed_result.path

        if path is None or len(path) < 1:
            path = '/'

        if parsed_result.query:
            path = f'{path}?{parsed_result.query}'

        is_ssl = parsed_result.scheme == 'https'
        host = parsed_result.hostname

        if self.query_callback is not None:
            msg, _ = await self.query_callback(host)
            _, rdata = msg.get_record((
                RecordType.A, 
                RecordType.AAAA
            ))

            host = rdata.data

        async with ConnectionHandler(
            host, 
            parsed_result.port, 
            hostname=parsed_result.hostname,
            ssl=is_ssl
        ) as conn:

            reader = conn.reader
            writer = conn.writer
            writer.write(
                f'{method} {path} HTTP/1.1\r\n'.encode()
            )

            merged_headers = {
                'host': parsed_result.hostname,
            }

            if headers:
                for key, value in headers.items():
                    merged_headers[key.lower()] = value

            if data:
                merged_headers['content-length'] = str(len(data))

            for key, value in merged_headers.items():
                writer.write(f'{key}: {value}\r\n'.encode())

            writer.write(b'\r\n')
            if data:
                writer.write(
                    data.encode()
                )

            await writer.drain()

            response_headers: Dict[str, str] = {}
            first_line = await reader.readline()
            _, status, message = first_line.strip().decode().split(
                ' ', 2
            )

            status = int(status)
            length: Union[int, None] = None
            if status == 204:
                length = 0

            while True:

                line = await reader.readline()
                line = line.strip().decode()

                if not line:
                    break

                key, _, value = line.partition(':')
                header_key = key.lower()

                response_headers[header_key] = value.strip()

            content_length = response_headers.get('content-length') 
            response_data: Union[str, None] = None

            if content_length:
                length = int(content_length)

                response_data = await reader.read(length)
            
            return HTTPResponse(
                url,
                status, 
                message, 
                response_headers, 
                response_data
            )

    async def query(
        self,
        url: str,
        name: str,
        qtype: Union[RecordType, int] = RecordType.A,
        method: Literal["GET", "POST"]="POST",
    ):
        dns_request = DNSMessage(qr=REQUEST)

        qtype_code = qtype
        if isinstance(qtype, RecordType):
            qtype_code = self.types_map.get_code_by_name(
                qtype.name
            )

        dns_request.qd = [
            Record(
                REQUEST, 
                name, 
                qtype_code
            )
        ]

        headers = {
            'accept': 'application/dns-message',
            'content-type': 'application/dns-message',
        }
        message = dns_request.pack()

        if method == 'GET':
            dns = base64.urlsafe_b64encode(
                message
            ).decode().rstrip(
                '='
            )

            params = {'dns': dns}
            data = None

        else:
            params = None
            data = message

        resp = await self.send_request(
            url,
            method=method,
            params=params,
            headers=headers,
            data=data
        )

        assert 200 <= resp.status < 300, f'Request error: {resp.status}'

        return DNSMessage.parse(resp.data)

    async def query_json(
        self,
        url: str,
        name: str,
        qtype: Union[RecordType, int] = RecordType.A
    ):

        if isinstance(qtype, RecordType):
            qtype = self.types_map.get_code_by_name(
                qtype.name
            )

        resp = await self.send_request(
            url,
            method="GET",
            params={
                'name': name,
                'qtype': qtype
            },
            headers={
                'accept': 'application/dns-json'
            }
        )

        assert 200 <= resp.status < 300, f'Request error: {resp.status}'

        data = json.loads(resp.data)

        return data
