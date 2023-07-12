import functools
from mercury_sync.models.request import Request
from pydantic import BaseModel
from typing import Callable, Union


class Middleware:

    def wrap(
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
        
        handler = self.modify(handler)
        
        @functools.wraps(handler)
        async def middleware_decorator(request: Request):

            
            response, run_next = await self.middleware(request)

            if run_next is False:
                return response
            
            return await handler(request)
        
        return middleware_decorator

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
        return handler
    
    async def middleware(
        self,
        request: Request
    ):
        raise NotImplementedError('Err. - middleware() is not implemented for base Middleware class.')