from __future__ import annotations
import re
from mercury_sync.discovery.dns.core.exceptions import InvalidServiceURLError
from mercury_sync.discovery.dns.core.record.record_data_types import (
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    PTRRecordData,
    SRVRecordData,
    TXTRecordData,
    RecordType
)
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    IPvAnyAddress
)

from typing import (
    Literal, 
    Tuple, 
    Optional, 
    Dict,
    Union,
    List
)



DomainProtocol = Literal["tcp", "udp"]
RecordTypeName = Literal["A", "AAAA", "CNAME", "PTR", "SRV", "TXT"]


service_pattern = re.compile(r'([a-zA-Z0-9\-]{1,256})?(\.?\_)([a-zA-Z0-9\-]{1,256})(\._)([udp|tcp]*)(\.)([a-zA-Z0-9\-]{1,256})(\.)([a-zA-Z0-9]{2,5})')


class DNSEntry(BaseModel):
    instance_name: Optional[StrictStr]
    service_name: StrictStr
    domain_protocol: DomainProtocol
    domain_name: StrictStr
    domain_priority: StrictInt=10
    domain_weight: StrictInt=0
    domain_port: Optional[StrictInt]
    domain_values: Dict[StrictStr, StrictStr]={}
    domain_targets: Optional[
        Tuple[
            Union[IPvAnyAddress, StrictStr]
        ]
    ]
    record_type: Optional[RecordType]
    record_types: List[RecordTypeName]=["PTR", "SRV", "TXT"]
    time_to_live: Union[StrictInt, StrictFloat]=-1


    @classmethod
    def to_segments(cls, url: str):

        if service_pattern.match(url) is None:
            raise InvalidServiceURLError(url)

        return [
            segment for segment in service_pattern.split(url) if segment.isalnum()
        ]

    def to_domain(
        self,
        record_type: RecordTypeName
    ):

        domain = self.domain_name

        if record_type == "SRV":
             domain = f'{self.instance_name}._{self.service_name}._{self.domain_protocol}.{domain}'

        elif record_type == "PTR":
            domain = f'{self.service_name}._{self.domain_protocol}.in-addr.arpa'

        return domain


    def to_data(
        self,
        record_type: RecordTypeName
    ):

        domain_target: Union[str, None] = None

        if self.domain_targets:
            domain_target = str(self.domain_targets[0])

        if record_type == "A":
            return ARecordData(domain_target)
        
        elif record_type == "AAAA":
            return AAAARecordData(domain_target)
        
        elif record_type == "CNAME":
            return CNAMERecordData(domain_target)
        
        elif record_type == "SRV":
            return SRVRecordData(
                self.domain_priority,
                self.domain_weight,
                self.domain_port,
                domain_target
            )
        
        elif record_type == "PTR" and self.instance_name:
            domain_target = f'{self.instance_name}._{self.service_name}._{self.domain_protocol}.{self.domain_name}'
            return PTRRecordData(domain_target)
        
        elif record_type == "PTR":
            domain_target = f'{self.instance_name}._{self.service_name}._{self.domain_protocol}.{self.domain_name}'
            return PTRRecordData(domain_target)
        
        else:
            domain_target_value = f'service={domain_target}'
            txt_values = [
                f'{key}={value}' for key, value in self.domain_values.items()
            ]

            txt_values.append(domain_target_value)

            txt_record_data = '\n'.join(txt_values)

            return TXTRecordData(txt_record_data)
        
    def to_record_data(self) -> List[
        Tuple[
            str, 
            Union[
                ARecordData,
                AAAARecordData,
                CNAMERecordData,
                PTRRecordData,
                SRVRecordData,
                TXTRecordData
            ]
        ]
    ]:
        return [
            (
                self.to_domain(record_type),
                self.to_data(record_type)
            ) for record_type in self.record_types
        ]
        
    @classmethod
    def from_record_data(
        self,
        record_name: str,
        record_data: Union[
            ARecordData,
            AAAARecordData,
            CNAMERecordData,
            SRVRecordData,
            TXTRecordData
        ],
        entry: DNSEntry
    ):
        
        if isinstance(
            record_data, 
            (
                ARecordData, 
                AAAARecordData, 
                CNAMERecordData
            )
        ):
            return DNSEntry(
                instance_name=entry.instance_name,
                service_name=entry.service_name.removeprefix('_'),
                domain_protocol=entry.domain_protocol.strip('_'),
                domain_name=record_name,
                domain_targets=(
                    record_data.data,
                ),
                record_type=record_data.rtype
            )
        
        elif isinstance(record_data, PTRRecordData):

            domain_segments = DNSEntry.to_segments(record_data.data)

            instance_name, service_name, domain_protocol = domain_segments[:3]
            domain_name = '.'.join(domain_segments[3:])

            return DNSEntry(
                instance_name=instance_name,
                service_name=service_name,
                domain_protocol=domain_protocol,
                domain_name=record_data.data,
                domain_targets=(
                    record_data.data,
                ),
                record_type=record_data.rtype
            )
        
        elif isinstance(record_data, SRVRecordData):

            domain_segments = DNSEntry.to_segments(record_name)
            instance_name, service_name, domain_protocol = domain_segments[:3]
            domain_name = '.'.join(domain_segments[3:])


            return DNSEntry(
                instance_name=instance_name,
                service_name=service_name,
                domain_protocol=domain_protocol,
                domain_name=domain_name,
                domain_port=record_data.port,
                domain_priority=record_data.priority,
                domain_weight=record_data.weight,
                domain_targets=(
                    record_data.hostname,
                ),
                record_type=record_data.rtype
            )
        
        else:

            txt_data = record_data.data.split("\n")

            record_values: Dict[str, str] = {}

            for txt_item in txt_data:
                key, value = txt_item.split("=")
                record_values[key] = value

            domain_target = record_values.get("service")

            return DNSEntry(
                instance_name=entry.instance_name,
                service_name=entry.service_name.removeprefix('_'),
                domain_protocol=entry.domain_protocol.removeprefix('_'),
                domain_name=record_name,
                domain_targets=(
                    domain_target,
                ),
                domain_values=record_values,
                record_type=record_data.rtype
            )


