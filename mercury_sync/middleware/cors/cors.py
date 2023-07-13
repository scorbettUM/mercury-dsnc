from mercury_sync.middleware.base import Middleware
from mercury_sync.models.response import Response
from mercury_sync.models.request import Request
from typing import Union, Optional, List, Literal, Tuple
from .cors_headers import CorsHeaders


class Cors(Middleware):

    def __init__(
        self,
        access_control_allow_origin: List[str]=None,
        access_control_allow_methods: List[
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
        ]=None,
        access_control_expose_headers: Optional[List[str]]=None,
        access_control_max_age: Optional[Union[int, float]]=None,
        access_control_allow_credentials: Optional[bool]=None,
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
        
        self.origins = self._cors_config.access_control_allow_origin
        self.cors_methods = self._cors_config.access_control_allow_methods
        self.cors_headers = self._cors_config.access_control_allow_headers

        self.allow_all_origins = '*' in self._cors_config.access_control_allow_origin
        
        super().__init__(
            self.__class__.__name__,
            methods=['OPTIONS'],
            response_headers=self._cors_config.to_headers()
        )
        
    async def __call__(self, request: Request) -> Tuple[
        Tuple[Response, int],
        bool
    ]:

        headers = request.headers
        method = request.method

        response_headers = {}

        origin = headers.get('origin')
        access_control_request_method = headers.get('access-control-request-method')
        access_control_request_headers = headers.get('access-control-request-headers')
        access_control_request_headers = headers.get("access-control-request-headers")

        if method == "OPTIONS" and access_control_request_method:

            failures: List[str] = []

            if self.allow_all_origins is False and origin not in self.origins:
                failures.append("origin")

            if access_control_request_method not in self.cors_methods:
                failures.append("method")
            
            if access_control_request_headers:
                headers = [
                    header for header in access_control_request_headers.split(
                        ','
                    ) if header.lower().strip() in self.cors_headers
                ]

                if len(headers) < 1:
                    failures.append("headers")
                
            if len(failures) > 0:

                failures_message = ', '.join(failures)

                return (
                    Response(
                        data=f"Disallowed CORS {failures_message}"
                    ),
                    401
                ), False
            
            has_cookie = headers.get('set-cookie')
            if self.allow_all_origins and has_cookie:
                response_headers['Access-Control-Allow_origin'] = origin

            elif origin in self.origins:
                response_headers['Access-Control-Allow_origin'] = origin
            
            return (
                Response(
                    headers=response_headers,
                    data=None
                ), 204), False

        return (Response(
            headers=response_headers,
            data=None
        ), 200), True