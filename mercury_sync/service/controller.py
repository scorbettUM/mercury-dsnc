import asyncio
import functools
import inspect
import multiprocessing as mp
import random
import signal
from collections import defaultdict
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor
)
from inspect import signature
from mercury_sync.connection.tcp.mercury_sync_tcp_connection import MercurySyncTCPConnection
from mercury_sync.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from mercury_sync.env import load_env, Env
from mercury_sync.models.error import Error
from mercury_sync.models.message import Message
from typing import (
    Optional, 
    List, 
    Literal, 
    Union, 
    Dict,
    get_args,
    Callable,
    AsyncIterable,
    Tuple
)


def handle_worker_loop_stop(signame, loop: asyncio.AbstractEventLoop):
    loop.stop()


def handle_loop_stop(signame, executor: Union[ProcessPoolExecutor, ThreadPoolExecutor]):
        try:
            
            executor.shutdown(cancel_futures=True)

        except BrokenPipeError:
            pass

        except RuntimeError:
            pass


def start_pool(
    udp_connection: MercurySyncUDPConnection,
    tcp_connection: MercurySyncTCPConnection,
    cert_path: Optional[str]=None,
    key_path: Optional[str]=None,
    engine_type: Literal["process", "thread", "async"] = None
):
    import asyncio

    try:
        import uvloop
        uvloop.install()

    except ImportError:
        pass

    udp_connection.connect(
        cert_path=cert_path,
        key_path=key_path
    )
    tcp_connection.connect(
        cert_path=cert_path,
        key_path=key_path
    )

    try:

        loop = asyncio.get_event_loop()

    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if engine_type == "process":
        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_worker_loop_stop(
                    signame,
                    loop
                )
            )  

    loop.run_forever()


class Controller:

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        workers: int=0,
        env: Optional[Env]=None,
        engine: Literal["process", "thread", 'async']="process"
    ) -> None:
    
        self.name = self.__class__.__name__
        self._instance_id = random.randint(0, 2**16)
        self._response_parsers: Dict[str, Message] = {}
        self._host_map: Dict[
            str, 
            Dict[
                Union[MercurySyncUDPConnection, MercurySyncTCPConnection],
                Tuple[str, int]
            ]
        ] = defaultdict(dict)

        if workers < 1:
            workers = 1

        self._workers = workers

        self.host = host
        self.port = port
        self.cert_path = cert_path
        self.key_path = key_path

        self._env = env
        self._engine: Union[ProcessPoolExecutor, ThreadPoolExecutor, None] = None 
        self._udp_queue: Dict[Tuple[str, int], asyncio.Queue] = defaultdict(asyncio.Queue)
        self._tcp_queue: Dict[Tuple[str, int], asyncio.Queue] = defaultdict(asyncio.Queue)
        self._cleanup_task: Union[asyncio.Task, None] = None

        if env is None:
            env = load_env(Env.types_map())

        self.engine_type = engine
        self._response_parsers: Dict[str, Message] = {}

        self.instance_ids = [
            self._instance_id + idx for idx in range(0, workers)
        ]

        port_pool_size = workers * 2
        self._udp_pool = [
            MercurySyncUDPConnection(
                self.host,
                self.port + idx,
                instance_id,
                env=env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, port_pool_size, 2)
            )
        ]

        self._tcp_pool = [
            MercurySyncTCPConnection(
                self.host,
                self.port + idx + 1,
                instance_id,
                env=env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, port_pool_size, 2)
            )
        ]

        methods = inspect.getmembers(self, predicate=inspect.ismethod)

        reserved_methods = [
            'connect',
            'send',
            'send_tcp',
            'stream',
            'stream_tcp',
            'close'
        ]


        controller_models: Dict[str, Message] = {}
        controller_methods: Dict[str, Callable[
            [Message],
            Message
        ]] = {}


        for _, method in methods:
            method_name = method.__name__

            not_internal = method_name.startswith('__') is False
            not_reserved = method_name not in reserved_methods
            is_server = hasattr(method, 'server_only')
            is_client = hasattr(method, 'client_only')

            rpc_signature = signature(method)

            if not_internal and not_reserved and is_server:

                
                for param_type in rpc_signature.parameters.values():
                    if param_type.annotation in Message.__subclasses__():

                        model = param_type.annotation
                        controller_models[method_name] = model

                controller_methods[method_name] = method

            elif not_internal and not_reserved and is_client:

                is_stream = inspect.isasyncgenfunction(method)

                if is_stream:

                    response_type = rpc_signature.return_annotation
                    args = get_args(response_type)

                    response_call_type: Tuple[int, Message] = args[0]
                    self._response_parsers[method.target] = get_args(response_call_type)[1]

                else:

                    response_type = rpc_signature.return_annotation
                    args = get_args(response_type)
                    response_model: Tuple[int, Message] = args[1]

                    self._response_parsers[method.target] = response_model

        self._parsers: Dict[str, Message] = {}
        self._events: Dict[str, Message] = {}

        for tcp_connection, udp_connection in zip(
            self._udp_pool,
            self._tcp_pool
        ):
            
            for method_name, model in controller_models.items():

                tcp_connection.parsers[method_name] = model
                udp_connection.parsers[method_name] = model
                self._parsers[method_name] = model

            for method_name, method in controller_methods.items():

                tcp_connection.events[method_name] = method
                udp_connection.events[method_name] = method
                self._events[method_name] = method
                

    async def start_server(
      self,
      cert_path: Optional[str]=None,
      key_path: Optional[str]=None      
    ):

        pool: List[asyncio.Future] = []

        loop = asyncio.get_event_loop()

        if self.engine_type == "process":
            engine = ProcessPoolExecutor(
                max_workers=self._workers,
                mp_context=mp.get_context(method='spawn')
            )

        elif self.engine_type == "thread":
            engine = ThreadPoolExecutor(max_workers=self._workers)

        if self.engine_type == 'process' or self.engine_type == 'thread':

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        engine
                    )
                )  

            for udp_connection, tcp_connection in zip(
                self._udp_pool,
                self._tcp_pool
            ):

                service_worker = loop.run_in_executor(
                    engine,
                    functools.partial(
                        start_pool,
                        udp_connection,
                        tcp_connection,
                        cert_path=cert_path,
                        key_path=key_path,
                        engine_type=self.engine_type
                    )
                )

                pool.append(service_worker) 

        else:
            for udp_connection, tcp_connection in zip(
                self._udp_pool,
                self._tcp_pool
            ):
                
                pool.append(
                    asyncio.create_task(
                        udp_connection.connect_async(
                            cert_path=cert_path,
                            key_path=key_path
                        )
                    )
                )

                pool.append(
                    asyncio.create_task(
                        tcp_connection.connect_async(
                            cert_path=cert_path,
                            key_path=key_path
                        )
                    )
                    )

        await asyncio.gather(*pool)

    async def start_client(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ):

        remote_pool: List[Message] = []
        pool_range = self._workers * 2

        for idx in range(0, pool_range, 2):
            remote_copy = Message(**{
                'host': remote.host,
                'port': remote.port + idx
            })

            remote_pool.append(remote_copy)

        for udp_connection, tcp_connection, remote_copy in zip(
            self._udp_pool,
            self._tcp_pool,
            remote_pool
        ):
      
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

                        
            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await self._udp_queue[(remote.host, remote.port)].put(udp_connection)
            await self._tcp_queue[(remote.host, remote.port)].put(tcp_connection)

            self._host_map[remote.__class__.__name__][udp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

            self._host_map[remote.__class__.__name__][tcp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

        for tcp_connection, remote_copy in zip(
            self._tcp_pool,
            remote_pool
        ):
            await tcp_connection.connect_client(
                    (remote_copy.host, remote_copy.port + 1),
                    cert_path=cert_path,
                    key_path=key_path
                )
            
    
    async def extend_client(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ) -> int:

        remote_pool: List[Message] = []
        pool_range = self._workers * 2

        for idx in range(0, pool_range, 2):
            remote_copy = Message(**{
                'host': remote.host,
                'port': remote.port + idx
            })

            remote_pool.append(remote_copy)

        port = max(self.port, remote.port) + pool_range

        udp_pool = [
            MercurySyncUDPConnection(
                self.host,
                port + idx,
                instance_id,
                env=self._env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, pool_range, 2)
            )
        ]

        tcp_pool = [
            MercurySyncTCPConnection(
                self.host,
                port + idx + 1,
                instance_id,
                env=self._env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, pool_range, 2)
            )
        ]

        for tcp_connection, udp_connection in zip(
            udp_pool,
            tcp_pool
        ):
            
            tcp_connection.parsers.update(self._parsers)
            udp_connection.parsers.update(self._parsers)

            tcp_connection.events.update(self._events)
            udp_connection.events.update(self._events)
            

        for udp_connection, tcp_connection, remote_copy in zip(
            udp_pool,
            tcp_pool,
            remote_pool
        ):
      
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await self._udp_queue[(remote.host, remote.port)].put(udp_connection)
            await self._tcp_queue[(remote.host, remote.port)].put(tcp_connection)

            self._host_map[remote.__class__.__name__][udp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

            self._host_map[remote.__class__.__name__][tcp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

        for tcp_connection, remote_copy in zip(
            tcp_pool,
            remote_pool
        ):
            await tcp_connection.connect_client(
                (remote_copy.host, remote_copy.port + 1),
                cert_path=cert_path,
                key_path=key_path
            )
            
        self._udp_pool.extend(udp_pool)
        self._tcp_pool.extend(tcp_pool)

        return port
    
    async def refresh_clients(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ) -> int:

        existing_udp_connections = self._udp_queue[(remote.host, remote.port)]
        existing_tcp_connections = self._tcp_queue[(remote.host, remote.port)]

        while existing_udp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_udp_connections.get()
            await connection.close()

        while existing_tcp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_tcp_connections.get()
            await connection.close()

        return await self.extend_client(
            remote,
            cert_path=cert_path,
            key_path=key_path
        )
    
    async def remove_clients(
        self,
        remote: Message 
    ):
        
        existing_udp_connections = self._udp_queue[(remote.host, remote.port)]
        existing_tcp_connections = self._tcp_queue[(remote.host, remote.port)]

        while existing_udp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_udp_connections.get()
            await connection.close()

        while existing_tcp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_tcp_connections.get()
            await connection.close()

        del self._udp_queue[(remote.host, remote.port)]
        del self._tcp_queue[(remote.host, remote.port)]

    async def send(
        self,
        event_name: str,
        message: Message
    ):
        connection: MercurySyncUDPConnection = await self._udp_queue[(message.host, message.port)].get()
        (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

        address = (
            host,
            port
        )

        shard_id, data = await connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        self._udp_queue[(message.host, message.port)].put_nowait(connection)

        return shard_id, response_data
    
    async def send_tcp(
        self,
        event_name: str,
        message: Message
    ):
        connection: MercurySyncTCPConnection = await self._tcp_queue[(message.host, message.port)].get()
        (host, port) = self._host_map.get(message.__class__.__name__).get(connection)
        address = (
            host,
            port + 1
        )

        shard_id, data = await connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        await self._tcp_queue[(message.host, message.port)].put(connection)

        return shard_id, response_data
    
    async def stream(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        
        async for connection in self._iter_udp_connections():
            (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

            address = (host, port)

            async for response in connection.stream(
                event_name,
                message.to_data(),
                address
            ):
                shard_id, data = response
                response_data = self._response_parsers.get(event_name)(
                    **data
                )

                yield shard_id, response_data

    async def stream_tcp(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        
        
        async for connection in self._iter_tcp_connections():
            (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

            address = (host, port + 1)

            async for response in connection.stream(
                event_name,
                message.to_data(),
                address
            ):
                shard_id, data = response

                if data.get('error'):
                    yield shard_id, Error(**data)

                response_data = self._response_parsers.get(event_name)(
                    **data
                )

                yield shard_id, response_data
    
    async def _iter_tcp_connections(self) -> AsyncIterable[MercurySyncUDPConnection]:
        for connection in enumerate(self._tcp_pool):
            yield connection

    async def _iter_udp_connections(self) -> AsyncIterable[MercurySyncTCPConnection]:
        for connection in enumerate(self._udp_pool):
            yield connection

    async def close(self) -> None:

        if self._engine:
            self._engine.shutdown(cancel_futures=True)

        await asyncio.gather(*[
            asyncio.create_task(
                udp_connection.close()
            ) for udp_connection in self._udp_pool
        ])

        await asyncio.gather(*[
            asyncio.create_task(
                tcp_connection.close()
            ) for tcp_connection in self._tcp_pool
        ])
