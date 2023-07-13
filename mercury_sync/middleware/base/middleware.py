from __future__ import annotations
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import (
    Any,
    Callable, 
    Union, 
    Dict, 
    Optional,
    List,
    Literal,
    Tuple
)
from .middleware_type import MiddlewareType
from .wrapper import Wrapper


class Middleware:

    def __init__(
        self,
        name: str,
        middleware_type: MiddlewareType=MiddlewareType.BEFORE,
        methods: Optional[
            List[
                Literal[
                    "GET",
                    "HEAD",
                    "OPTIONS",
                    "POST",
                    "PUT",
                    "PATCH",
                    "DELETE",
                    "TRACE"
                ]
            ]
        ]=None,
        response_headers: Dict[str, str]={}
    ) -> None:
        
        self.name = name
        self.methods = methods
        self.response_headers = response_headers
        self.middleware_type = middleware_type

    def __call__(self, request: Request) -> Tuple[
        Tuple[Response, int],
        bool
    ]:
        raise NotImplementedError('Err. __call__() should not be called on base Middleware class.')

    def wrap(
        self,
        handler: Callable[
            [Request],
            Union[
                BaseModel,
                str,
                None
            ]
        ]
    ):
        
        
        wrapper = Wrapper(
            self.name,
            handler,
            methods=self.methods,
            response_headers=self.response_headers,
            middleware_type=self.middleware_type
        )

        wrapper.run = self

        wrapper.run.response_headers.update(wrapper.response_headers)
        
        return wrapper
    
    async def run(
        self,
        request: Request
    ):
        raise NotImplementedError('Err. - middleware() is not implemented for base Middleware class.')
    