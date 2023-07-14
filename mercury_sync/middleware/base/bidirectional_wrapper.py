
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import (
    Callable, 
    Union, 
    Dict, 
    Optional,
    List,
    Literal,
    TypeVar
)
from .base_wrapper import BaseWrapper
from .types import MiddlewareType
from .types import (
    Handler,
    MiddlewareHandler
)


T = TypeVar('T')


class BidirectionalWrapper(BaseWrapper):

    def __init__(
        self,
        name: str,
        handler: Handler,
        middleware_type: MiddlewareType=MiddlewareType.BIDIRECTIONAL,
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
        
        super().__init__()

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
        self.wraps = isinstance(handler, BaseWrapper)

        if self.handler.response_headers and self.response_headers:
            self.handler.response_headers = {}

        self.pre: Optional[MiddlewareHandler] = None
        self.post: Optional[MiddlewareHandler] = None

        self.middleware_type = middleware_type

    async def __call__(
        self, 
        request: Request
    ):

        (request, middleware_status), run_next = await self.pre(request)

        if run_next is False:
            return Response(
                request.path,
                request.method,
                headers=request.headers,
                response=request.data
            ), middleware_status
        
        response, status = await self.handler(request)

        (response, middleware_status), run_next = await self.post(
            request,
            response,
            status
        )

        if self.response_headers:
            response.headers.update(response)

        if run_next is False:
            return response, middleware_status

        return response, status