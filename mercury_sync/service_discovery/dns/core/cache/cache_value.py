import time
from mercury_sync.service_discovery.dns.core.record.record_data_types import (
    RecordData
)
from mercury_sync.service_discovery.dns.core.record import (
    Record, 
    RecordType
)
from typing import Dict, Iterable, Tuple


class CacheValue:
    def __init__(self):
        self.data: Dict[RecordData, Dict[Tuple[int, RecordData], Record]] = {}

    def check_ttl(self, record: Record):
        return record.ttl < 0 or record.timestamp + record.ttl >= time.time()

    def get(self, qtype: RecordType) -> Iterable[Record]:

        if qtype == RecordType.ANY:
            for qt in self.data.keys():
                yield from self.get(qt)
        
        results = self.data.get(qtype)
        if results is not None:

            keys = list(results.keys())
            for key in keys:
                record = results[key]

                if self.check_ttl(record):
                    yield record
                    
                else:
                    results.pop(key, None)

    def add(self, record: Record):
        if self.check_ttl(record):
            results = self.data.setdefault(record.qtype, {})
            results[record.data] = record

