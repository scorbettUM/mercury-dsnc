
            

import functools
from mercury_sync.service import Service
from mercury_sync.models.message import Message


def stream(
    call_name: str, 
    as_tcp: bool=False
):

    def wraps(func):

        func.client_only = True

        @functools.wraps(func)
        async def decorator(
            *args,
            **kwargs
        ):
            connection: Service = args[0]

            if as_tcp:

                async for data in func(*args, **kwargs):
                    async for response in connection.stream_tcp(
                        call_name,
                        data
                    ):
                        yield response


            else:
                async for data in func(*args, **kwargs):
                    async for response in connection.stream(
                        call_name,
                        data
                    ):

                        yield response
            
        return decorator
    
    return wraps
