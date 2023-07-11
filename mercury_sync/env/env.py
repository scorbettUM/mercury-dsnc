from ipaddress import IPv4Address
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat,
    IPvAnyAddress
)
from typing import (
    Dict, 
    Union,
    Callable,
    Literal
)


PrimaryType = Union[str, int, float, bytes, bool]


class Env(BaseModel):
    MERCURY_SYNC_HTTP_RATE_LIMIT_STRATEGY: Literal[
        "none", 
        "global", 
        "endpoint", 
        "ip",
        "ip-endpoint",
        "custom"
    ]="none"
    MERCURY_SYNC_HTTP_RATE_LIMITER_TYPE: Literal[
        "adaptive",
        "token-bucket",
        "leaky-bucket",
        "sliding-window",
        "cpu-adaptive"
    ]="sliding-window"
    MERCURY_SYNC_HTTP_CPU_LIMIT: Union[StrictFloat, StrictInt]=50
    MERCURY_SYNC_HTTP_RATE_LIMIT_BACKOFF_RATE: StrictInt=10
    MERCURY_SYNC_HTTP_RATE_LIMIT_BACKOFF: StrictStr='1s'
    MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD: StrictStr='1s'
    MERCURY_SYNC_HTTP_RATE_LIMIT_REQUESTS: StrictInt=100
    MERCURY_SYNC_HTTP_RATE_LIMIT_DEFAULT_REJECT: StrictBool=True
    MERCURY_SYNC_USE_HTTP_MSYNC_ENCRYPTION: StrictBool=False
    MERCURY_SYNC_USE_HTTP_SERVER: StrictBool=False
    MERCURY_SYNC_USE_HTTP_AND_TCP_SERVERS: StrictBool=False
    MERCURY_SYNC_TCP_CONNECT_RETRIES: StrictInt=3
    MERCURY_SYNC_CLEANUP_INTERVAL: StrictStr='10s'
    MERCURY_SYNC_MAX_CONCURRENCY: StrictInt=2048
    MERCURY_SYNC_AUTH_SECRET: StrictStr
    MERCURY_SYNC_MULTICAST_GROUP: IPvAnyAddress='224.1.1.1'

    @classmethod
    def types_map(self) -> Dict[str, Callable[[str], PrimaryType]]:
        return {
            'MERCURY_SYNC_HTTP_RATE_LIMIT_BACKOFF_RATE': int,
            'MERCURY_SYNC_HTTP_RATE_LIMIT_BACKOFF': str,
            'MERCURY_SYNC_HTTP_CPU_LIMIT': float,
            'MERCURY_SYNC_HTTP_RATE_LIMIT_STRATEGY': str,
            'MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD': str,
            'MERCURY_SYNC_USE_TCP_SERVER': lambda value: True if value.lower() == 'true' else False,
            'MERCURY_SYNC_HTTP_RATE_LIMIT_REQUESTS': int,
            'MERCURY_SYNC_HTTP_RATE_LIMIT_DEFAULT_REJECT': lambda value: True if value.lower() == 'true' else False,
            'MERCURY_SYNC_USE_HTTP_MSYNC_ENCRYPTION': lambda value: True if value.lower() == 'true' else False,
            'MERCURY_SYNC_USE_HTTP_SERVER': lambda value: True if value.lower() == 'true' else False,
            'MERCURY_SYNC_TCP_CONNECT_RETRIES': int,
            'MERCURY_SYNC_CLEANUP_INTERVAL': str,
            'MERCURY_SYNC_MAX_CONCURRENCY': int,
            'MERCURY_SYNC_AUTH_SECRET': str,
            'MERCURY_SYNC_MULTICAST_GROUP': str
        }