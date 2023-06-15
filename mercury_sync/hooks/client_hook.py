import functools
from mercury_sync.service import Service


def client(
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
                return await connection.send_tcp(
                    call_name,
                    await func(*args, **kwargs)
                )

            else:
                return await connection.send(
                    call_name,
                    await func(*args, **kwargs)
                )
            
        return decorator
    
    return wraps
