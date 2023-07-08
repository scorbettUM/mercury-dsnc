import functools
from typing import (
    Optional, 
    List, 
    Literal,
    Dict
)


def server(
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
    response_headers: Optional[Dict[str, str]]=None,
    as_http=False
):

    def wraps(func):

        func.server_only = True
        func.path = path
        func.methods = methods
        func.as_http =as_http
        func.response_headers = response_headers

        @functools.wraps(func)
        def decorator(
            *args,
            **kwargs
        ):
            return func(*args, **kwargs)
        
        return decorator
    
    return wraps
