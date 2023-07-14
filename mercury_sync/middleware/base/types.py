from enum import Enum
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import (
    Any,
    Callable, 
    Union, 
    Tuple,
    Coroutine
)


class MiddlewareType(Enum):
    BIDIRECTIONAL='BIDIRECTIONAL'
    UNIDIRECTIONAL_BEFORE='UNIDIRECTIONAL_BEFORE'
    UNIDIRECTIONAL_AFTER='UNIDIRECTIONAL_AFTER'


RequestHandler = Callable[
    [Request],
    Coroutine[
        Any,
        Any,
        Tuple[
            Union[
                Response,
                BaseModel,
                str,
                None
            ],
            int
        ]
    ]
]

MiddlewareHandler = Callable[
    [
        Request,
        Response,
        int
    ],
    Coroutine[
        Any, 
        Any, 
        Tuple[
            Tuple[Response, int],
            bool
        ]
    ]
]


Handler = Union[RequestHandler, MiddlewareHandler]