from __future__ import annotations
import asyncio
import functools
from collections import deque
from mercury_sync.connection.addresses import SubnetRange
from typing import (
    Dict, 
    Tuple,
    Union,
    Set,
    Deque
)
from .connection import Connection


class ConnectionPool:
    pools: Dict[
        Tuple[str, int, bool, str],
        ConnectionPool
    ] = {}

    @classmethod
    def get(
        cls,
        host: str,
        port: int,
        hostname: str = None,
        max_size: int = 6,
        ssl: bool = False
    ):

        if port is None and ssl:
            port = 443

        elif port is None:
            port = 80

        key = (
            host, 
            port, 
            ssl, 
            hostname
        )

        pool = cls.pools.get(key)

        if pool is None:
            pool = cls(host, port, ssl, hostname, max_size)
            cls.pools[key] = pool

        return pool

    @classmethod
    def close_all(cls):
        for pool in list(cls.pools.values()):
            pool.close()

        cls.pools.clear()

    def __init__(
        self, 
        host: str, 
        port: int, 
        ssl: bool, 
        hostname: Union[str, None],
        max_size: int
    ):
        self.addr = (
            host, 
            port
        )

        self.host = host
        self.port = port
        self.ssl = ssl
        self.hostname = hostname
        self.key = host, port, ssl, hostname
        self.tasks: Set[asyncio.Future] = set()
        self.connections = set()
        self.requests: Deque[asyncio.Future] = deque()
        self.max_size = max_size
        self.size = 0

        self._subnet = SubnetRange(host)

    def on_connection(self, result):
        reader, writer = result
        self.connections.add(Connection(reader, writer))
        self.check()

    def on_connection_error(self, exc):
        self.size -= 1

    def check(self):

        while len(self.requests) > 0 and len(self.connections) > 0:
            future = self.requests.popleft()
            conn = self.connections.pop()
            self.ensure_task(self.acquire_connection(conn, future))

        for _ in self.requests:
            if self.size >= self.max_size:
                break

            self.size += 1

            self.ensure_task(
                asyncio.open_connection(
                    *self.addr,
                    ssl=self.ssl,
                    server_hostname=self.hostname
                ),
                self.on_connection, 
                self.on_connection_error
            )

    async def _open_connection(self):
        for host_ip in self._subnet:
            try:
                result = await asyncio.open_connection(
                    host_ip,
                    self.port,
                    ssl=self.ssl,
                    server_hostname=self.hostname
                )

                self.host = host_ip
                self.addr = (
                    host_ip,
                    self.port
                )

                return result
            
            except OSError:
                pass

    def check_later(self):
        loop = asyncio.get_event_loop()
        loop.call_soon(self.check)

    def ensure_task(
        self, 
        coro, 
        on_success=None, 
        on_error=None
    ):
        task: asyncio.Future = asyncio.ensure_future(coro)
        self.tasks.add(task)

        

        task.add_done_callback(
            lambda: self.on_done(
                task,
                on_success,
                on_error
            )
        )

    def on_done(
        self,
        task: asyncio.Future,
        on_success,
        on_error
    ):
        
        try:

            result = task.result()
            if on_success is not None: 
                on_success(result)

        except Exception as exc:

            if on_error is not None: 
                on_error(exc)

        self.tasks.remove(
            task
        )

    async def acquire_connection(
        self, 
        conn: Connection, 
        future: asyncio.Future
    ):
        try:
            await conn.writer.drain()
        except Exception:
            self.discard_connection(conn)
            if not future.done():
                self.requests.appendleft(future)
            return False
        if conn.timer:
            conn.timer.cancel()
            conn.timer = None
        if future.done():
            self.put_connection(conn)

        else:
            future.set_result(conn)

        return True

    async def get_connection(self):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self.requests.append(future)
        self.check_later()
        try:
            result = await future
            return result
        except asyncio.CancelledError:
            future.cancel()
            raise

    def put_connection(self, conn: Connection):
        self.connections.add(conn)
        loop = asyncio.get_running_loop()
        conn.timer = loop.call_later(
            10, 
            functools.partial(
                self.discard_connection, 
                conn
            )
        )
        
        self.check_later()

    def discard_connection(self, conn: Connection):
        conn.writer.close()
        if conn.timer: conn.timer.cancel()
        self.connections.discard(conn)
        self.size -= 1

    def close(self):
        for task in self.tasks:
            task.cancel()

        for conn in list(self.connections):

            self.discard_connection(conn)
        for fut in self.requests:
            fut.cancel()

        self.tasks.clear()
        self.connections.clear()
        self.requests.clear()
        self.pools.pop(self.key, None)