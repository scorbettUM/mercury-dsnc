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
    "errored", 
    "removed", 
    "non-responsive"
]

class HealthCheck(Message):
    target_host: Optional[StrictStr]
    target_port: Optional[StrictInt]
    shard_ids: Optional[List[StrictInt]]
    source_host: StrictStr
    source_port: StrictInt
    status: HealthStatus