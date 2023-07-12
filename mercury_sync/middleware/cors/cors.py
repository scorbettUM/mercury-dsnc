from mercury_sync.middleware.base import Middleware
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import Callable, Union, Optional, List, Literal
from .cors_headers import CorsHeaders


class Cors(Middleware):

    def __init__(
        self,
        access_control_allow_origin: List[str],
        access_control_expose_headers: Optional[List[str]]=None,
        access_control_max_age: Optional[Union[int, float]]=None,
        access_control_allow_credentials: Optional[bool]=None,
        access_control_allow_methods: Optional[
            List[
                Literal[
                    "GET",
                    "HEAD",
                    "OPTIONS",
                    "POST",
                    "PUT",
                    "PATCH",
                    "DELETE"
                ]
            ]
        ]=None,
        access_control_allow_headers: Optional[List[str]]=None
    ) -> None:
        
        self._cors_config = CorsHeaders(
            access_control_allow_origin=access_control_allow_origin,
            access_control_expose_headers=access_control_expose_headers,
            access_control_max_age=access_control_max_age,
            access_control_allow_credentials=access_control_allow_credentials,
            access_control_allow_methods=access_control_allow_methods,
            access_control_allow_headers=access_control_allow_headers,
        )

        self.cors_headers = self._cors_config.to_headers()

    def modify(
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
        handler.methods.append("OPTIONS")
        return handler

    async def middleware(self, request: Request):
        if request.method == "OPTIONS":
            
            return (None, 204), False

        return (None, 200), True