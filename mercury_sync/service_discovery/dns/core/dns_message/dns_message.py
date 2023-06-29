import io
import struct
from typing import (
    List, 
    Dict, 
    Tuple, 
    Union,
    Iterable
)
from mercury_sync.service_discovery.dns.core.record import (
    Record,
    RecordType
)
from .dns_error import DNSError
from .utils import get_bits


REQUEST = 0
RESPONSE = 1


class DNSMessage:
    def __init__(
        self, 
        qr=RESPONSE, 
        qid=0, 
        o=0, 
        aa=0, 
        tc=0, 
        rd=1, 
        ra=1, 
        r=0
    ):
        self.qr = qr  # 0 for request, 1 for response
        self.qid = qid  # id for UDP package
        self.o = o  # opcode: 0 for standard query
        self.aa = aa  # Authoritative Answer
        self.tc = tc  # TrunCation, will be updated on .pack()
        self.rd = rd  # Recursion Desired for request
        self.ra = ra  # Recursion Available for response
        self.r = r  # rcode: 0 for success
        self.qd: List[Record] = []
        self.an: List[Record] = []  # answers
        self.ns: List[Record] = []  # authority records, aka nameservers
        self.ar: List[Record] = []  # additional records

    def __bool__(self):
        return any(map(len, (self.an, self.ns)))

    def __getitem__(self, i):
        return self.an[i]

    def __iter__(self):
        return iter(self.an)

    def __repr__(self):
        return '<DNSMessage type=%s qid=%d r=%d QD=%s AN=%s NS=%s AR=%s>' % (
            self.qr, self.qid, self.r, self.qd, self.an, self.ns, self.ar)

    def pack(self, size_limit: int = None):
        z = 0
        names: Dict[str, int] = {}
        buf = io.BytesIO()
        buf.seek(12)
        tc = 0
        for group in self.qd, self.an, self.ns, self.ar:
            if tc: break
            for rec in group:
                offset = buf.tell()
                brec = rec.pack(names, offset)
                if size_limit is not None and offset + len(brec) > size_limit:
                    tc = 1
                    break
                buf.write(brec)
        self.tc = tc
        buf.seek(0)
        buf.write(
            struct.pack('!HHHHHH', self.qid, (self.qr << 15) + (self.o << 11) +
                        (self.aa << 10) + (self.tc << 9) + (self.rd << 8) +
                        (self.ra << 7) + (z << 4) + self.r, len(self.qd),
                        len(self.an), len(self.ns), len(self.ar)))
        return buf.getvalue()

    @staticmethod
    def parse_entry(
        qr: int, 
        data: bytes, 
        cursor_posiition: int,
        n: int
    ) -> Tuple[int, List[Record]]:
        
        results: List[Record] = []

        for _ in range(n):
            record = Record(qr)
            cursor_posiition = record.parse(data, cursor_posiition)

            results.append(record)

        return cursor_posiition, results

    @classmethod
    def parse(cls, data: bytes, qid: bytes = None):

        (
            rqid, 
            x, 
            qd, 
            an, 
            ns, 
            ar
        ) = struct.unpack('!HHHHHH', data[:12])

        if qid is not None and qid != rqid:
            raise DNSError(-1, 'Transaction ID mismatch')
        
        r, x = get_bits(x, 4)  # rcode: 0 for no error

        z, x = get_bits(x, 3)  # reserved

        ra, x = get_bits(x, 1)  # recursion available

        rd, x = get_bits(x, 1)  # recursion desired

        tc, x = get_bits(x, 1)  # truncation

        aa, x = get_bits(x, 1)  # authoritative answer

        o, x = get_bits(x, 4)  # opcode
        
        qr, x = get_bits(x, 1)  # qr: 0 for query and 1 for response

        ans = DNSMessage(
            qr, 
            rqid, 
            o, 
            aa, 
            tc, 
            rd, 
            ra, 
            r
        )

        cursor_position, ans.qd = ans.parse_entry(
            REQUEST, 
            data, 
            12, 
            qd
        )

        cursor_position, ans.an = ans.parse_entry(
            RESPONSE, 
            data, 
            cursor_position, 
            an
        )

        cursor_position, ans.ns = ans.parse_entry(
            RESPONSE, 
            data, 
            cursor_position, 
            ns
        )

        _, ans.ar = ans.parse_entry(
            RESPONSE, 
            data, 
            cursor_position, 
            ar
        )

        return ans

    def get_record(self, qtypes: Union[RecordType, Iterable[RecordType]]):
        '''Get the first record of qtype defined in `qtypes` in answer list.
        '''
        if isinstance(qtypes, RecordType):
            qtypes = qtypes

        for item in self.an:
            if item.qtype in qtypes:
                return item.data
