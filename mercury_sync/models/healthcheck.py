from pydantic import (
    StrictStr,
    StrictInt
)
from typing import Literal, Optional, List
from .message import Message


HealthStatus = Literal[
    "initializing",
    "healthy", 
    "suspect", 
    "degraded", 
    "failed"
]

class HealthCheck(Message):
    target_host: Optional[StrictStr]
    target_port: Optional[StrictInt]
    target_status: Optional[StrictStr]
    source_host: StrictStr
    source_port: StrictInt
    source_status: Optional[StrictStr]
    status: HealthStatus