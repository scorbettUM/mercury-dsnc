
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
    TypeVar,
    Tuple,
    Coroutine
)
from .middleware_type import MiddlewareType


T = TypeVar('T')


class Wrapper:

    def __init__(
        self,
        name: str,
        handler: Callable[
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
        ],
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
        responses: Optional[
            Dict[
                int,
                BaseModel
            ]
        ]=None,
        serializers: Optional[
            Dict[
                int,
                Callable[
                    ...,
                    str
                ]
            ]
        ]=None,
        response_headers: Optional[
            Dict[str, str]
        ]=None
    ) -> None:
        
        self.name = name
        self.path  = handler.path
        self.methods: List[
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
        ] = handler.methods

        if methods:
            self.methods.extend(methods)

        self.response_headers: Union[
            Dict[str, str],
            None
        ] = handler.response_headers

        if self.response_headers and response_headers:
            self.response_headers.update(response_headers)

        elif response_headers:
            self.response_headers = response_headers

        self.responses = responses
        self.serializers = serializers
        self.limit = handler.limit

        self.handler = handler
        self.is_wrapped = isinstance(handler, Wrapper)

        if self.handler.response_headers and self.response_headers:
            self.handler.response_headers = {}

        self.run: Optional[
            Callable[
                [Request],
                Coroutine[
                    Any, 
                    Any, 
                    Tuple[
                        Tuple[Response, int],
                        bool
                    ]
                ]
            ]
        ] = None

        self.middleware_type = middleware_type
        self._run_before = middleware_type == MiddlewareType.BEFORE

    async def __call__(
        self, 
        request: Request
    ):


        if self._run_before:
            (response, middleware_status), run_next = await self.run(request)

            if self.response_headers:
                response.headers.update(self.response_headers)

            if run_next is False:
                return response, middleware_status
            
            result, status = await self.handler(request)

        else:
            result, status = await self.handler(request)

            (response, middleware_status), run_next = await self.run(
                request,
                result,
                status
            )

            if self.response_headers:
                response.headers.update(self.response_headers)

            if run_next is False:
                return response, middleware_status

        if self.is_wrapped:
            response.headers.update(result.headers)
            response.data = result.data
            return response, status
        
        response.data = result
        return response, status