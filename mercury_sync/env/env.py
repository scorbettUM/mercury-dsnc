from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from typing import (
    Dict, 
    Union,
    Callable
)


PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr='10s'
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt=2048
    MERCURY_SYNC_AUTH_SECRET: StrictStr

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_CLEANUP_INTERVAL': str,
            'MERCURY_SYNC_MAX_CONCURRENCY': int,
            'MERCURY_SYNC_AUTH_SECRET': str
        }