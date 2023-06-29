from pydantic import (
    BaseModel,
    StrictStr,
    IPvAnyAddress
)

from typing import Literal, Tuple


class DNSEntry(BaseModel):
    domain_name: StrictStr
    record_type: Literal["A", "AAAA", "CNAME", "TXT"]
    domain_targets: Tuple[IPvAnyAddress]