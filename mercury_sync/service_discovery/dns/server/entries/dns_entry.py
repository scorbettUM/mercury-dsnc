from mercury_sync.service_discovery.dns.core.record.record_data_types import (
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    SRVRecordData,
    TXTRecordData
)
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    IPvAnyAddress,
)

from typing import (
    Literal, 
    Tuple, 
    Optional, 
    List, 
    Dict,
    Union
)


class DNSEntry(BaseModel):
    domain_types: List[Literal["_tcp", "_http", "_udp"]]=[]
    domain_name: StrictStr
    domain_priority: StrictInt=10
    domain_weight: StrictInt=0
    domain_port: Optional[StrictInt]
    domain_values: Dict[StrictStr, StrictStr]={}
    domain_targets: Tuple[IPvAnyAddress]
    record_type: Literal["A", "AAAA", "CNAME", "SRV", "TXT"]

    def to_domain(self):

        domain = self.domain_name

        if self.record_type == "SRV":
            for domain_type in self.domain_types[::-1]:
                domain = f'{domain_type}.{domain}'

        return domain


    def to_data(self):

        domain_target = str(self.domain_targets[0])

        if self.record_type == "A":
            return ARecordData(domain_target)
        
        elif self.record_type == "AAAA":
            return AAAARecordData(domain_target)
        
        elif self.record_type == "CNAME":
            return CNAMERecordData(domain_target)
        
        elif self.record_type == "SRV":
            return SRVRecordData(
                self.domain_priority,
                self.domain_weight,
                self.domain_port,
                domain_target
            )
        
        else:
            domain_target_value = f'service={domain_target}'
            txt_values = [
                f'{key}={value}' for key, value in self.domain_values.items()
            ]

            txt_values.append(domain_target_value)

            txt_record_data = '\n'.join(txt_values)

            return TXTRecordData(txt_record_data)
        
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
            return DNSEntry(
                domain_name=record_name,
                domain_targets=(
                    record_data.data,
                ),
                record_type=record_data.rtype.name
            )
        
        elif isinstance(record_data, SRVRecordData):

            valid_segments = ["_udp", "_tcp", "http"]

            domain_types: List[Literal["_udp", "_tcp", "http"]] = []
            domain_segments = record_name.split(".")

            domain_types = [
                segment for segment in domain_segments if segment in valid_segments
            ]

            return DNSEntry(
                domain_types=domain_types,
                domain_name=record_name,
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

            return DNSEntry(
                domain_name=record_name,
                domain_targets=(
                    domain_target,
                ),
                domain_values=record_values,
                record_type=record_data.rtype.value
            )


