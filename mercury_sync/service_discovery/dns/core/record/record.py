import io
import struct
import time
from typing import Dict, Tuple
from .record_data_types import (
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    MXRecordData,
    NAPTRRecordData,
    NSRecordData,
    PTRRecordData,
    RecordData,
    RecordType,
    RecordTypesMap,
    SOARecordData,
    SRVRecordData,
    TXTRecordData,
    UnsupportedRecordData

)
from mercury_sync.service_discovery.dns.core.record.record_data_types.utils import (
    load_domain_name, 
    pack_domain_name, 
    pack_string
)


REQUEST = 0
RESPONSE = 1
MAXAGE = 3600000



class Record:

    record_types: Dict[
        RecordType,
        RecordData
    ] = {
        RecordType.A: ARecordData,
        RecordType.AAAA: AAAARecordData,
        RecordType.CNAME: CNAMERecordData,
        RecordType.MX: MXRecordData,
        RecordType.NAPTR: NAPTRRecordData,
        RecordType.NS: NSRecordData,
        RecordType.PTR: PTRRecordData,
        RecordType.SOA: SOARecordData,
        RecordType.SRV: SRVRecordData,
        RecordType.TXT: TXTRecordData
    }

    def __init__(
        self,
        q: int = RESPONSE,
        name: str = '',
        qtype: RecordType = RecordType.ANY,
        qclass: int = 1,
        ttl: int = 0,
        data: Tuple[int, RecordData] = None
    ):
        self.q = q
        self.name = name
        self.qtype = qtype
        self.qclass = qclass
        if q == RESPONSE:
            self.ttl = ttl  # 0 means item should not be cached
            self.data = data
            self.timestamp = int(time.time())

        self.types_map = RecordTypesMap()

    def __repr__(self):

        type_name = self.types_map.names_mapping.get(self.qtype)

        if self.q == REQUEST:
            return f'<Record type=request qtype={type_name} name={self.name}>'
        else:
            return f'<Record type=response qtype={type_name} name={self.name} ttl={self.ttl} data={self.data}>'
    
    @classmethod
    def create_rdata(
        cls,
        qtype: RecordType, 
        *args
    ) -> RecordData:
        
        record_data = cls.record_types.get(qtype)

        if record_data is None:
            return UnsupportedRecordData(
                qtype,
                *args
            )

        return record_data(
            *args
        )

    @classmethod
    def load_rdata(
        cls,
        qtype: RecordType, 
        data: bytes, 
        cursor_position: int,
        size: int
    ) -> Tuple[int, RecordData]:
        '''Load RData from a byte sequence.'''
        record_data = cls.record_types.get(qtype)
        if record_data is None:
            return UnsupportedRecordData.load(
                data, 
                cursor_position, 
                size, 
                qtype
            )
        
        return record_data.load(
            data, 
            cursor_position, 
            size
        )

    def copy(
        self, 
        **kwargs
    ):
        return Record(
            q=kwargs.get('q', self.q),
            name=kwargs.get('name', self.name),
            qtype=kwargs.get('qtype', self.qtype),
            qclass=kwargs.get('qclass', self.qclass),
            ttl=kwargs.get('ttl', self.ttl),
            data=kwargs.get('data', self.data)
        )

    def parse(
        self, 
        data: bytes, 
        cursor_position: int
    ):
        cursor_position, self.name = load_domain_name(
            data, 
            cursor_position
        )

        qtype, self.qclass = struct.unpack(
            '!HH', 
            data[cursor_position:cursor_position + 4]
        )

        self.qtype = self.types_map.types_by_code.get(
            qtype
        )

        cursor_position += 4
        if self.q == RESPONSE:
            self.timestamp = int(time.time())
            self.ttl, dl = struct.unpack(
                '!LH', 
                data[cursor_position:cursor_position + 6]
            )

            cursor_position += 6

            _, self.data = Record.load_rdata(
                self.qtype, 
                data, 
                cursor_position, 
                dl
            )

            cursor_position += dl

        return cursor_position

    def pack(
        self, 
        names, 
        offset=0
    ):
        buf = io.BytesIO()

        buf.write(
            pack_domain_name(
                self.name, 
                names, 
                offset
            )
        )

        buf.write(
            struct.pack(
                '!HH', 
                self.qtype.value, 
                self.qclass
            )
        )

        if self.q == RESPONSE:

            if self.ttl < 0:
                ttl = MAXAGE

            else:
                now = int(time.time())
                self.ttl -= now - self.timestamp

                if self.ttl < 0:
                    self.ttl = 0

                self.timestamp = now
                ttl = self.ttl

            buf.write(
                struct.pack(
                    '!L', 
                    ttl
                )
            )

            data_str = b''.join(
                self.data.dump(
                    names, 
                    offset + buf.tell()
                )
            )

            buf.write(
                pack_string(
                    data_str, 
                    '!H'
                )
            )

        return buf.getvalue()
