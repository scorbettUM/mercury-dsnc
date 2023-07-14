from gzip import compress
from mercury_sync.middleware.base import (
    Middleware,
    MiddlewareType
)
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import (
    Dict, 
    Union,
    Tuple,
    Callable
)


class BidirectionalGZipCompressor(Middleware):

    def __init__(
        self, 
        compression_level: int=9,
        serializers: Dict[
            str,
            Callable[
                [
                    Union[
                        Response,
                        BaseModel,
                        str,
                        None
                    ]
                ],
                Union[
                    str,
                    None
                ]
            ]
        ]={}
    ) -> None:
        super().__init__(
            self.__class__.__name__,
            middleware_type=MiddlewareType.BIDIRECTIONAL
        )

        self.compression_level = compression_level
        self.serializers = serializers

    async def __pre__(
        self,
        request: Request
    ):
        try:
            request.content = compress(
                request.content,
                compresslevel=self.compression_level
            )

            return (
                request,
                200
            ), True

        except Exception as e:
            return (
                Response(
                    request.path,
                    request.method,
                    data=str(e)
                ),
                500
            ), False
        
    async def __post__(
        self, 
        request: Request,
        response: Union[
            BaseModel,
            str,
            None
        ],
        status: int
    ) -> Tuple[
        Tuple[Response, int], 
        bool
    ]:
        try:
            
            if response is None:
                return (
                    Response(
                        request.path,
                        request.method,
                        data=response
                    ),
                    status
                ), True
            
            elif isinstance(response, str):
                return (
                    Response(
                        request.path,
                        request.method,
                        headers={
                            'content-encoding': 'gzip'
                        },
                        data=compress(
                            response.encode(),
                            compresslevel=self.compression_level
                        ).decode()
                    ),
                    status
                ), True
            
            else:
                serialized = self.serializers[request.path](response)

                return (
                    Response(
                        request.path,
                        request.method,
                        headers={
                            'content-encoding': 'gzip',
                            'content-type': 'application/json'
                        },
                        data=compress(
                            serialized.encode(),
                            compresslevel=self.compression_level
                        ).decode()
                    ),
                    status
                ), True
            
        except KeyError:
            return (
                Response(
                    request.path,
                    request.method,
                    data=f'No serializer for {request.path} found.'
                ),
                500
            ), False

        except Exception as e:
            return (
                Response(
                    request.path,
                    request.method,
                    data=str(e)
                ),
                500
            ), False