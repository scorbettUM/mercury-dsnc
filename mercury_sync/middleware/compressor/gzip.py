from gzip import compress, decompress
from mercury_sync.middleware.base import Middleware
from mercury_sync.middleware.base.middleware_type import MiddlewareType
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import (
    Dict, 
    List, 
    Literal, 
    Optional, 
    Union,
    Tuple,
    Callable
)


class GZipCompressor(Middleware):

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
            middleware_type=MiddlewareType.AFTER
        )

        self.compression_level = compression_level
        self.serializers = serializers

    async def __call__(
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
                    Response(data=response),
                    status
                ), True
            
            elif isinstance(response, str):
                return (
                    Response(
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
                    data=f'No serializer for {request.path} found.'
                ),
                500
            ), False

        except Exception as e:
            return (
                Response(
                    data=str(e)
                ),
                500
            ), False