import socket
from mercury_sync.service_discovery.dns.core.record import RecordType
from typing import Optional
from urllib.parse import urlparse
from .exceptions import (
    InvalidHost,
    InvalidIP
)
from .host import Host


class Address:

    default_ports = {
        'tcp': 53,
        'udp': 53,
        'tcps': 853,
        'http': 80,
        'https': 443,
    }

    def __init__(
        self, 
        hostinfo: Host, 
        protocol: str, 
        path: str=None
    ):
        self.hostinfo = hostinfo
        self.protocol = protocol
        self.path = path
        
        self.ip_type = self.get_ip_type(
            self.hostinfo.hostname
        )

    def __str__(self):
        protocol = self.protocol or '-'
        host = self.hostinfo.host
        path = self.path or ''
        return f'{protocol}://{host}{path}'

    def __eq__(self, other):
        return str(self) == str(other)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self))

    def copy(self):

        host = Host(self.hostinfo.netloc)

        return Address(
            host, 
            self.protocol, 
            self.path
        )

    def to_addr(self):
        return self.hostinfo.hostname, self.hostinfo.port

    def to_ptr(self):
        if self.ip_type is RecordType.A:
            return '.'.join(reversed(
                self.hostinfo.hostname.split('.'))) + '.in-addr.arpa'
        
        raise InvalidIP(self.hostinfo.hostname)
    
    def get_ip_type(
        self,
        hostname: str
    ):

        if ':' in hostname:
            # ipv6
            try:
                socket.inet_pton(socket.AF_INET6, hostname)
            except OSError:
                raise InvalidHost(hostname)
            
            return RecordType.AAAA
        
        try:
            socket.inet_pton(socket.AF_INET, hostname)
        except OSError:
            # domain name
            pass
        else:
            return RecordType.A

    @classmethod
    def parse(
        cls, 
        value: str, 
        port: Optional[int]=None,
        protocol: str='udp', 
        allow_domain: bool=False
    ):

        if isinstance(value, Address):
            return value.copy()
        
        if '://' not in value:
            value = '//' + value

        parsed = urlparse(
            value, 
            scheme=protocol
        )
        
        hostinfo = Host(parsed.netloc)

        if hostinfo.port is None and port is None:
            hostinfo.port = cls.default_ports.get(
                parsed.scheme, 
                53
            )

        else:
            hostinfo.port = str(port)

        addr = Address(
            hostinfo, 
            parsed.scheme, 
            parsed.path
        )
        
        if not allow_domain and addr.ip_type is None:
            raise InvalidHost(
                hostinfo.hostname,
                'Err. - allow_domain must be true to to pass domain names.'
            )
        
        return addr
