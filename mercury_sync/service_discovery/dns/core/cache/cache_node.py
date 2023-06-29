from mercury_sync.service_discovery.dns.core.record.record_data_types import (
    RecordData
)
from mercury_sync.service_discovery.dns.core.record import (
    Record, 
    RecordType
)
from typing import Dict, Iterable, Union
from .cache_value import CacheValue


class CacheNode:
    def __init__(self):
        self.children: Dict[str, CacheNode] = {}
        self.data = CacheValue()

    def get(self, fqdn: str, touch: bool = False):
        current = self
        keys = reversed(fqdn.split('.'))
        for key in keys:
            child = current.children.get(key)

            if child is None:
                child = current.children.get('*')

            if child is None and touch is False:
                return None

            elif child is None and touch:
                child = CacheNode()
                current.children[key] = child
                
            current = child
        return current.data

    def query(
        self, 
        fqdn: str, 
        qtype: Union[RecordType, Iterable[RecordType]]
    ):
        if isinstance(qtype, RecordType):
            value = self.get(fqdn)
            if value is not None:
                yield from value.get(qtype)
        else:
            for record_type in qtype:
                yield from self.query(fqdn, record_type)

    def add(
        self,
        fqdn: str = None,
        qtype: RecordType = None,
        data: Union[RecordData, bytes, Iterable] = None,
        ttl=-1,
        record: Record = None
    ):
        if record is None:

            if isinstance(data, bytes):
                _, rdata = Record.load_rdata(qtype, data, 0, len(data))

            elif isinstance(data, RecordData):
                rdata = data

            else:
                rdata = Record.create_rdata(qtype, *data)

            record = Record(
                name=fqdn, 
                data=rdata, 
                qtype=qtype, 
                ttl=ttl
            )

        value = self.get(record.name, True)
        value.add(record)

    def iter_values(self) -> Iterable[Record]:

        yield from self.data.get(RecordType.ANY)

        for child in self.children.values():
            yield from child.iter_values()
