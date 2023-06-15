from __future__ import annotations
from pydantic import BaseModel, StrictStr, StrictInt
from typing import Optional

class Message(BaseModel):
    host: Optional[StrictStr]
    port: Optional[StrictInt]

    def to_data(self):
        return self.dict(
            exclude={
                'host',
                'port'
            }
        )
    
