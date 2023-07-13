import zstandard
from base64 import b64decode
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


class ZstandardCompressor(Middleware):

    def __init__(
        self, 
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

        self.serializers = serializers
        self._compressor = zstandard.ZstdCompressor()

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

                compressed_data: bytes = self._compressor.compress(
                    response.encode()
                )

                return (
                    Response(
                        headers={
                            'content-encoding': 'zstd'
                        },
                        data=b64decode(
                            compressed_data
                        ).decode()
                    ),
                    status
                ), True
            
            else:

                serialized = self.serializers[request.path](response)
                compressed_data: bytes = self._compressor.compress(
                    serialized.encode()
                )

                return (
                    Response(
                        headers={
                            'content-encoding': 'zstd'
                        },
                        data=b64decode(
                            compressed_data
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