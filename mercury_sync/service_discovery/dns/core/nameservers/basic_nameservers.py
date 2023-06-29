from mercury_sync.service_discovery.dns.core.address import Address
from typing import List, Iterable
from .exceptions import NoNameServer


class BasicNameServers:
    def __init__(
        self, 
        port: int,
        nameservers: List[str]=[], 
        **kwargs
    ):
        self.data = [
            Address.parse(
                nameserver, 
                port=port,
                protocol='udp', 
                allow_domain=True
            ) for nameserver in nameservers
        ]
        
        super().__init__(**kwargs)

    def __bool__(self):
        return len(self.data) > 0

    def __iter__(self):
        return iter(self.data)

    def __repr__(self):
        return '<NameServers [%s]>' % ','.join(
            map(str, self.data)
        )

    def iter(self) -> Iterable[Address]:
        if not self.data: 
            raise NoNameServer()
        
        return iter(self.data)

    def success(self, item):
        pass

    def fail(self, item):
        pass
