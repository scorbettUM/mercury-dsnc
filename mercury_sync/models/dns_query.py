from mercury_sync.discovery.dns.core.record.record_data_types import (
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    PTRRecordData,
    SRVRecordData,
    TXTRecordData
)
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat,
    IPvAnyAddress,
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
RecordType = Literal["A", "AAAA", "CNAME", "PTR", "SRV", "TXT"]
RecordData = Union[
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    PTRRecordData,
    SRVRecordData,
    TXTRecordData
]



class DNSQuery(BaseModel):
    query_id: StrictInt
    instance_name: Optional[StrictStr]
    application_protocol: Optional[StrictStr]
    domain_protocol: Optional[DomainProtocol]
    domain_name: StrictStr
    domain_priority: StrictInt=10
    domain_weight: StrictInt=0
    domain_port: Optional[StrictInt]
    domain_values: Dict[StrictStr, StrictStr]={}
    domain_targets: Tuple[IPvAnyAddress]
    record_types: List[RecordType]=["PTR", "SRV", "TXT"]
    time_to_live: Optional[Union[StrictInt, StrictFloat]]

    def to_domain(
        self,
        record_type: RecordType
    ):

        domain = self.domain_name

        if record_type == "SRV":
             domain = f'{self.instance_name}._{self.application_protocol}._{self.domain_protocol}.{domain}'

        elif record_type == "PTR":
            domain_target = str(self.domain_targets[0])
            domain = f'{domain_target}.in-addr.arpa'

        return domain


    def to_data(
        self,
        record_type: RecordType
    ):

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
        
        elif record_type == "PTR":
            return PTRRecordData(self.domain_name)
        
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
        ]
    ):
        
        if isinstance(
            record_data, 
            (
                ARecordData, 
                AAAARecordData, 
                CNAMERecordData,
            )
        ):
            return DNSQuery(
                domain_name=record_name,
                domain_targets=(
                    record_data.data,
                ),
                record_type=record_data.rtype.name
            )
        
        elif isinstance(record_data, SRVRecordData):

            domain_segments = record_name.split(".")
            
            instance_name = None
            application_protocol = None

            if len(domain_segments) == 4:
                instance_name, application_protocol, domain_protocol = domain_segments[:3]
            elif len(domain_segments) == 3:
                instance_name, domain_protocol = domain_segments[:2]

            else:
                domain_protocol = domain_segments[0]

            domain_name = domain_segments[-1]


            return DNSQuery(
                instance_name=instance_name,
                application_protocol=application_protocol,
                domain_protocol=domain_protocol,
                domain_name=domain_name,
                domain_port=record_data.port,
                domain_priority=record_data.priority,
                domain_weight=record_data.weight,
                domain_targets=(
                    record_data.hostname,
                ),
                record_type=record_data.rtype.name
            )
        
        else:

            txt_data = record_data.data.split("\n")

            record_values: Dict[str, str] = {}

            for txt_item in txt_data:
                key, value = txt_item.split("=")
                record_values[key] = value

            domain_target = record_values.get("service")

            return DNSQuery(
                domain_name=record_name,
                domain_targets=(
                    domain_target,
                ),
                domain_values=record_values,
                record_type=record_data.rtype.value
            )


