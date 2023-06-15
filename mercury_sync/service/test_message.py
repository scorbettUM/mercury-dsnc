from mercury_sync.models.message import Message
from pydantic import StrictStr
from typing import Optional

class TestMessage(Message):
    message: Optional[StrictStr]

