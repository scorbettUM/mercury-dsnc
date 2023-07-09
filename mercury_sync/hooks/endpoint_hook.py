import functools
from pydantic import BaseModel
from typing import (
    Optional, 
    List, 
    Literal,
    Dict,
    Callable,
    Tuple
)


def endpoint(
    path: Optional[str]="/",
    method: Literal[
        "GET",
        "HEAD",
        "OPTIONS",
        "POST",
        "PUT",
        "PATCH",
        "DELETE"
    ]="GET",
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
    response_headers: Optional[Dict[str, str]]=None,
    rate_limit: Optional[
        Tuple[
            int, 
            str
        ]
    ]=None
):

    def wraps(func):

        func.server_only = True
        func.path = path
        func.method = method
        func.as_http = True
        func.response_headers = response_headers
        func.responses = responses
        func.serializers = serializers
        func.rate_limit = rate_limit

        @functools.wraps(func)
        def decorator(
            *args,
            **kwargs
        ):
            return func(*args, **kwargs)
        
        return decorator
    
    return wraps
