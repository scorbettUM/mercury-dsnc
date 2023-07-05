from pydantic import (
    StrictStr,
    StrictInt,
    Json
)
from typing import Dict, Optional, Union
from .message import Message


class HTTPMessage(Message):
    protocol: StrictStr
    status: StrictInt
    status_message: Optional[StrictStr]
    headers: Dict[StrictStr, StrictStr]
    data: Union[Json, StrictStr]