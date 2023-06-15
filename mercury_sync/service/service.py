import asyncio
import random
import inspect
import signal
from inspect import signature
from mercury_sync.connection.tcp.mercury_sync_tcp_connection import MercurySyncTCPConnection
from mercury_sync.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from mercury_sync.models.message import Message
from typing import (
    Tuple, 
    Dict, 
    AsyncIterable,
    Type,
    Any,
    Optional,
    get_args
)



def handle_loop_stop(signame, tcp_connection: MercurySyncTCPConnection):
        try:
            tcp_connection.close()

        except BrokenPipeError:
            pass 

        except RuntimeError:
            pass


class Service:
    
    def __init__(
        self,
        host: str,
        port: int
    ) -> None:
        self.name = self.__class__.__name__
        self._instance_id = random.randint(0, 2**16)
        self._response_parsers: Dict[str, Message] = {}


        self._udp_connection = MercurySyncUDPConnection(
            host,
            port,
            self._instance_id
        )

        self._tcp_connection = MercurySyncTCPConnection(
            host,
            port + 1,
            self._instance_id
        )

        self._host_map: Dict[str, Tuple[str, int]] = {}    

        methods = inspect.getmembers(self, predicate=inspect.ismethod)

        reserved_methods = [
            'start',
            'connect',
            'send',
            'send_tcp',
            'stream',
            'stream_tcp',
            'close'
        ]

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

                        self._tcp_connection.parsers[method_name] = model
                        self._udp_connection.parsers[method_name] = model
  
                self._tcp_connection.events[method_name] = method
                self._udp_connection.events[method_name] = method

            elif not_internal and not_reserved and is_client:

                is_stream = inspect.isasyncgenfunction(method)

                if is_stream:

                    response_type = rpc_signature.return_annotation
                    args = get_args(response_type)
                    response_model = args[0]

                    self._response_parsers[method.target] = response_model

                else:

                    response_model = rpc_signature.return_annotation
                    self._response_parsers[method.target] = response_model



        self._loop = asyncio.get_event_loop()

        for signame in ('SIGINT', 'SIGTERM'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._tcp_connection
                )
            )

    def start(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        self._tcp_connection.connect(
            cert_path=cert_path,
            key_path=key_path
        )
        self._udp_connection.connect(cert_path=cert_path)

    async def connect(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        address = (remote.host, remote.port)
        self._host_map[remote.__class__.__name__] = address

        await self._tcp_connection.connect_client(
            (remote.host, remote.port + 1),
            cert_path=cert_path,
            key_path=key_path
        )

    async def send(
        self, 
        event_name: str,
        message: Message
    ):
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        shard_id, data = await self._udp_connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )
        return shard_id, response_data
    
    async def send_tcp(
        self,
        event_name: str,
        message: Message
    ):
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        shard_id, data = await self._tcp_connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )
        return shard_id, response_data
    
    async def stream(
        self,
        event_name: str,
        message: Message
    ):
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        async for response in self._udp_connection.stream(
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
    ):
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        async for response in self._tcp_connection.stream(
            event_name,
            message.to_data(),
            address
        ):
            shard_id, data = response
            response_data = self._response_parsers.get(event_name)(
                **data
            )

            yield shard_id, response_data
    
    async def close(self):
        self._tcp_connection.close()
