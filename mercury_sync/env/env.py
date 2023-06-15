import psutil
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from typing import (
    Optional, 
    Dict, 
    Union,
    Callable
)


PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERURY_SYNC_AUTH_SECRET: StrictStr

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERURY_SYNC_AUTH_SECRET': str
        }