from .connection import Connection
from .connection_pool import ConnectionPool


class ConnectionHandler:
    def __init__(
        self, 
        host: str,
        port: int,
        hostname: str = None,
        max_size: int = 6,
        ssl: bool=False
    ):
        self.pool = ConnectionPool.get(
            host,
            port,
            hostname=hostname,
            max_size=max_size,
            ssl=ssl
        )
        self.conn = None

    async def __aenter__(self) -> Connection:
        self.conn = await self.pool.get_connection()
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        if exc is None:
            self.pool.put_connection(self.conn)
        else:
            self.pool.discard_connection(self.conn)
