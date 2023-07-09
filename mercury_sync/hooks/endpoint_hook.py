import functools
from pydantic import BaseModel
from typing import (
    Optional, 
    List, 
    Literal,
    Dict,
    Callable
)


def endpoint(
    path: Optional[str]="/",
    methods: List[
        Literal[
            "GET",
            "HEAD",
            "OPTIONS",
            "POST",
            "PUT",
            "PATCH",
            "DELETE"
        ]
    ]=["GET"],
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
    response_headers: Optional[Dict[str, str]]=None
):

    def wraps(func):

        func.server_only = True
        func.path = path
        func.methods = methods
        func.as_http = True
        func.response_headers = response_headers
        func.responses = responses
        func.serializers = serializers

        @functools.wraps(func)
        def decorator(
            *args,
            **kwargs
        ):
            return func(*args, **kwargs)
        
        return decorator
    
    return wraps
