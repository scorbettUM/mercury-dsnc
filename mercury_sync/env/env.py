from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat
)
from typing import (
    Dict, 
    Union,
    Callable
)


PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_BOOT_WAIT: StrictStr='5s'
    MERCURY_SYNC_MAX_SUSPECT_TIMEOUT: StrictStr='10s'
    MERCURY_SYNC_HEALTH_CHECK_TIMEOUT: StrictStr='1s'
    MERCURY_SYNC_REGISTRATION_TIMEOUT: StrictStr='1m'
    MERCURY_SYNC_HEALTH_POLL_INTERVAL: StrictFloat='0.2s'
    MERCURY_SYNC_INDIRECT_CHECK_NODES: StrictInt=1
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr='10s'
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt=2048
    MERCURY_SYNC_AUTH_SECRET: StrictStr

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_MAX_SUSPECT_TIMEOUT': str,
            'MERCURY_SYNC_BOOT_WAIT': str,
            'MERCURY_SYNC_REGISTRATION_TIMEOUT': str,
            'MERCURY_SYNC_HEALTH_POLL_INTERVAL': str,
            'MERCURY_SYNC_INDIRECT_CHECK_NODES': int,
            'MERCURY_SYNC_CLEANUP_INTERVAL': str,
            'MERCURY_SYNC_MAX_CONCURRENCY': int,
            'MERCURY_SYNC_AUTH_SECRET': str
        }