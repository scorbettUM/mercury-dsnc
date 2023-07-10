from mercury_sync.env.time_parser import TimeParser
from pydantic import (
    BaseModel,
    IPvAnyAddress,
    StrictStr,
    StrictInt
)
from typing import (
    Optional, 
    Callable, 
    Literal,
    List,
    Union
)

from .request import Request


HTTPMethod = Literal[
    "GET",
    "HEAD",
    "OPTIONS",
    "POST",
    "PUT",
    "PATCH",
    "DELETE"
]


class Limit(BaseModel):
    request_limit: StrictInt
    request_period: StrictStr='1s'
    limiter_type: Optional[
        Literal[
            "token-bucket",
            "leaky-bucket",
            "sliding-window"
        ]
    ]
    limit_key: Optional[
        Callable[
            [
                Request,
                IPvAnyAddress,
            ],
            str
        ]
    ]
    rules: Optional[
        List[
            Callable[
                [
                    Request,
                    IPvAnyAddress,

                ],
                bool
            ]
        ]
    ]

    def period(self):
        return TimeParser(self.request_period).time

    def get_key(
        self, 
        request: Request,
        ip_address: IPvAnyAddress,
        default: str='default'
    ):

        if self.limit_key is None:
            return default
        
        return self.limit_key(
            request,
            ip_address
        )

    def matches(
        self,
        request: Request,
        ip_address: IPvAnyAddress
    ):

        if self.rules is None:
            return True
        
        matches_rules = False
        
        for rule in self.rules:
            matches_rules = rule(
                request,
                ip_address
            )

        return matches_rules

