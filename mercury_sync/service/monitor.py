import asyncio
import random
import time
import traceback
from collections import defaultdict, deque
from mercury_sync.env import Env, load_env
from mercury_sync.env.time_parser import TimeParser
from mercury_sync.hooks.client_hook import client
from mercury_sync.hooks.server_hook import server
from mercury_sync.models.healthcheck import HealthCheck, HealthStatus
from mercury_sync.models.ticket import Ticket
from mercury_sync.types import Call
from typing import Optional, Dict, Tuple, List, Deque, Union
from .controller import Controller


async def cancel(pending_item: asyncio.Task) -> None:
    pending_item.cancel()
    if not pending_item.cancelled():
        try:
            await pending_item

        except asyncio.CancelledError:
            pass

        except asyncio.IncompleteReadError:
            pass


class Monitor(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        workers: int=0,
    ) -> None:
        
        if workers <= 1:
            engine = 'async'

        else:
            engine = 'process'

        env = load_env(Env.types_map())

        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            workers=workers,
            env=env,
            engine=engine
        )

        self.status: HealthStatus = 'initializing'
        

        self.error_context: Optional[str] = None
        self.registration_timeout = TimeParser(env.MERCURY_SYNC_REGISTRATION_TIMEOUT).time
        self.boot_wait = TimeParser(env.MERCURY_SYNC_BOOT_WAIT).time

        self._healthchecks: Dict[str, asyncio.Task] = {}
        self._registered: Dict[int, Tuple[str, int]] = {}
        self._running = False
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time
        self._poll_interval = TimeParser(env.MERCURY_SYNC_HEALTH_POLL_INTERVAL).time
        self._poll_timeout = TimeParser(env.MERCURY_SYNC_HEALTH_CHECK_TIMEOUT).time
        self._max_suspect_timeout = TimeParser(env.MERCURY_SYNC_MAX_SUSPECT_TIMEOUT).time
        self._check_nodes_count = env.MERCURY_SYNC_INDIRECT_CHECK_NODES

        self._local_health_monitor: Optional[asyncio.Task] = None
        self._monitoring: Dict[Tuple[str, int], bool] = defaultdict(lambda: False)
        self._waiter: Optional[asyncio.Future] = None
        self._active_checks_queue: Deque[asyncio.Task] = deque()
        self._failed_nodes: Deque[Tuple[str, int]] = deque()
        self._suspect_nodes: Dict[Tuple[str, int], List[int]] = {}
        self._removed_nodes: Dict[Tuple[str, int], List[int]] = {}

        self._active_probes: Dict[Tuple[str, int], bool] = {}
        self._failed_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._suspect_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._investigating_nodes: Dict[Tuple[str, int], Dict[Tuple[str, int]]] = defaultdict(dict)

    @server()
    async def update_active(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        if self._suspect_nodes.get((target_host, target_port)):
            del self._suspect_nodes[(target_host, target_port)]

        if target_host != self.host and target_port != self.port:
            self._monitoring[(target_host, target_port)] = True

            await self.push_new_node(
                target_host,
                target_port,
                self.host,
                self.port,
                self.status,
                error_context=self.error_context
            )

        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            error=self.error_context
        )

    @server()
    async def update_suspect(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        target_host = healthcheck.target_host
        target_port = healthcheck.target_port
        shard_ids = healthcheck.shard_ids

        self._suspect_nodes[(target_host, target_port)] = shard_ids
        self._monitoring[(target_host, target_port)] = False

        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            error=self.error_context
        )


    @server()
    async def send_indirect_check(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        try:

            investigation_update = self._push_investigating_update(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                target_host=healthcheck.target_host,
                target_port=healthcheck.target_port,
                timeout=self._poll_timeout
            )

            indirect_probe = asyncio.wait_for(
                self.push_health_update(
                    healthcheck.target_host,
                    healthcheck.target_port,
                    healthcheck.status,
                    error_context=healthcheck.error
                ),
                timeout=self._poll_timeout
            )

            for task in asyncio.as_completed([
                investigation_update,
                indirect_probe
            ]):
                result: Union[Tuple[int, HealthCheck], None] = await task

                if isinstance(result, tuple):

                    _, response = result

                    return HealthCheck(
                        host=healthcheck.source_host,
                        port=healthcheck.source_port,   
                        source_host=response.source_host,
                        source_port=response.source_port,
                        status=response.status,
                        error=response.error
                    )

        except asyncio.TimeoutError:

            self._monitoring[(healthcheck.target_host, healthcheck.target_port)] = False

            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                source_host=healthcheck.target_host,
                source_port=healthcheck.target_port,
                status='suspect',
                error='timeout'
            )
        

    @server()
    async def register_new_node(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        host = healthcheck.source_host
        port = healthcheck.source_port

        not_self = host != self.host and port != self.port

        if not_self:

            suspect_tasks = dict(self._suspect_tasks)
            suspect_task = suspect_tasks.get((host, port))

            if suspect_task:
                await cancel(suspect_task)
                del suspect_tasks[(host, port)]
                
                self._suspect_tasks = suspect_tasks
            
            await self.extend_client(
                HealthCheck(
                    host=host,
                    port=port,
                    source_host=self.host,
                    source_port=self.port,
                    status=healthcheck.status,
                    error=healthcheck.error
                )
            )

            self._monitoring[(host, port)] = True

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            error=self.error_context,
            status=self.status
        )
        
    @server()
    async def register_health_update(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        monitor_exists_status = self._monitoring.get((source_host, source_port))

        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.get((source_host, source_port))

        if suspect_task:
            await cancel(suspect_task)
            del suspect_tasks[(source_host, source_port)]

            self._suspect_tasks = suspect_tasks
        
        if healthcheck.status == 'investigating':

            target_host = healthcheck.target_host
            target_port =healthcheck.target_port

            self._investigating_nodes[(target_host, target_port)].update({
                (source_host, source_port): healthcheck.status
            })

        if not monitor_exists_status:

            monitoring = [
                address for address, monitoring_status in self._monitoring.items() if monitoring_status
            ]
            
            for host, port in monitoring:

                await self.push_new_node(
                    host,
                    port,
                    source_host,
                    source_port,
                    healthcheck.status,
                    error_context=healthcheck.error
                )

        if monitor_exists_status is None:

            await self.extend_client(
                HealthCheck(
                    host=source_host,
                    port=source_port,
                    source_host=self.host,
                    source_port=self.port,
                    status=healthcheck.status,
                    error=healthcheck.error
                )
            )

            self._monitoring[(source_host, source_port)] = True

        elif monitor_exists_status is False:

            await self.refresh_clients(
                HealthCheck(
                    host=source_host,
                    port=source_port,
                    source_host=self.host,
                    source_port=self.port,
                    status=healthcheck.status,
                    error=healthcheck.error
                )
            )

            self._monitoring[(source_host, source_port)] = True

        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            error=self.error_context,
            status=self.status
        )

    @client('register_health_update')
    async def push_health_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            error=error_context,
            status=health_status
        )
    
    @client('register_health_update', as_tcp=True)
    async def push_tcp_health_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            error=error_context,
            status=health_status
        )
    
    @client('register_new_node')
    async def push_new_node(
        self,
        host: str,
        port: int,
        source_host: str,
        source_port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            source_host=source_host,
            source_port=source_port,
            error=error_context,
            status=health_status
        )
    
    @client('send_indirect_check')
    async def request_indirect_check(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None

    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            target_host=target_host,
            target_port=target_port,
            source_host=self.host,
            source_port=self.port,
            error=error_context,
            status=health_status
        )
    
    @client('update_suspect')
    async def submit_suspect_node(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        shard_ids: List[int],
        health_status: HealthStatus,
        error_context: Optional[str]=None

    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            target_host=target_host,
            target_port=target_port,
            shard_ids=shard_ids,
            source_host=self.host,
            source_port=self.port,
            error=error_context,
            status=health_status
        )
    
    @client('update_active')
    async def submit_active_node(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            target_host=target_host,
            target_port=target_port,
            source_host=self.host,
            source_port=self.port,
            error=error_context,
            status=health_status
        )
        
    
    async def start(self):

        await self.start_server()
        await asyncio.sleep(self.boot_wait)

    async def register(
        self,
        host: str,
        port: int
    ) -> Call[Ticket]:
        
        await asyncio.wait_for(
            asyncio.create_task(
                self.start_client(
                    HealthCheck(
                        host=host,
                        port=port,
                        source_host=self.host,
                        source_port=self.port,
                        status=self.status
                    ),
                    cert_path=self.cert_path,
                    key_path=self.key_path
                )
            ),
            timeout=self.registration_timeout
        )

        self._monitoring[(host, port)] = True

        self.status = 'healthy'
        self._running = True
        
        self._healthchecks[(host, port)] = asyncio.create_task(
            self.start_health_monitor()
        )
        
        self.confirmation_task = asyncio.create_task(
            self.cleanup_pending_checks()
        )
    
    async def _push_investigating_update(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        timeout: float
    ):
        try:
            await asyncio.sleep(0.8 * timeout)

            await asyncio.wait_for(
                self.push_health_update(
                    host=host,
                    port=port,
                    target_host=target_host,
                    target_port=target_port,
                    health_status='investigating',
                    error_context=self.error_context
                ),
                timeout=self._poll_timeout
            )

        except asyncio.TimeoutError:
            if not self._active_probes.get((host, port)):
                self._active_probes[(host, port)] = True
                self._failed_nodes.append((
                    host,
                    port
                ))

                self._failed_tasks[(host, port)] = asyncio.create_task(
                    self._probe_timed_out_node()
                )
            
            self._monitoring[(host, port)] = False
    
    async def _update_suspect_nodes(
        self,
        confirmation_members: List[Tuple[str, int]],
        successful_requests: Dict[Tuple[str, int], int]
    ):
        for node_host, node_port in confirmation_members:

            request_failed = successful_requests.get((node_host, node_port)) is None
            not_active_probe = self._active_probes.get((node_host, node_port)) is None

            if request_failed and not_active_probe:
                self._active_probes[(node_host, node_port)] = True
                self._failed_nodes.append((
                    node_host,
                    node_port
                ))

                self._monitoring[(node_host, node_port)] = False

                self._failed_tasks[(node_host, node_port)] = asyncio.create_task(
                    self._probe_timed_out_node()
                )

    async def _run_healthcheck(self, host: str, port: int):

        try:

            await asyncio.wait_for(
                self.push_health_update(
                    host,
                    port,
                    self.status,
                    error_context=self.error_context
                ),
                timeout=self._poll_timeout
            )

        except asyncio.TimeoutError:
            if not self._active_probes.get((host, port)):
                self.status = 'investigating'

                self._active_probes[(host, port)] = True
                self._failed_nodes.append((
                    host,
                    port
                ))

                self._failed_tasks[(host, port)] = asyncio.create_task(
                    self._probe_timed_out_node()
                )
            
            self._monitoring[(host, port)] = False

    async def _probe_timed_out_node(self):

        host, port = self._failed_nodes.pop()
        
        confirmation_members = self._get_confirmation_members()

        healthchecks, suspect_count = await self._request_indirect_probe(
            host,
            port,
            confirmation_members
        )

        indirect_ack_count = len(self._investigating_nodes[(host, port)])

        if len(confirmation_members) > indirect_ack_count:
            # If we have received more results from other
            # nodes than nacks we may be a degraded node.
            self.status = 'degraded'

        elif suspect_count >= len(confirmation_members):

            self._suspect_nodes[(host, port)] = [
                shard_id for shard_id, _ in healthchecks
            ]

            await asyncio.gather(*[
                asyncio.wait_for(
                    self.submit_suspect_node(
                        node_host,
                        node_port,
                        host,
                        port,
                        self._suspect_nodes.get(
                            (host, port),
                            []
                        ),
                        self.status,
                        error_context=self.error_context
                    ),
                    timeout=self._poll_timeout
                ) for node_host, node_port in confirmation_members
            ], return_exceptions=True)

            self._suspect_tasks[(host, port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )

        else:
            self._monitoring[(host, port)] = True 

            await self._update_suspect_nodes(
                confirmation_members,
                self._investigating_nodes[(host, port)]
            )

        del self._investigating_nodes[(host, port)]
            
    def _get_confirmation_members(self) -> List[Tuple[str, int]]:
        confirmation_members = [
            address for address, monitoring_status in self._monitoring.items() if monitoring_status
        ]

        confirmation_candidates_count = len(confirmation_members) 

        if confirmation_candidates_count < self._check_nodes_count:
            self._check_nodes_count = confirmation_candidates_count

        confirmation_members = random.sample(
            confirmation_members, 
            self._check_nodes_count
        )

        return confirmation_members

    async def _request_indirect_probe(
        self,
        host: str,
        port: int,
        confirmation_members: List[Tuple[str, int]]
    ) -> Tuple[List[Call[HealthCheck]], int]:

        if len(confirmation_members) < 1:
            requested_checks = [
                asyncio.create_task(
                    self.push_tcp_health_update(
                        host,
                        port,
                        self.status,
                        error_context=self.error_context
                    )
                )
            ]
        
        else:
            requested_checks = [
                asyncio.create_task(
                    self.request_indirect_check(
                        node_host,
                        node_port,
                        host,
                        port,
                        self.status,
                        error_context=self.error_context
                    )
                ) for node_host, node_port in confirmation_members
            ]

            requested_checks.append(
                asyncio.create_task(
                    self.push_tcp_health_update(
                        host,
                        port,
                        self.status,
                        error_context=self.error_context
                    )
                )
            )


        check_tasks: Tuple[List[asyncio.Task], List[asyncio.Task]] = await asyncio.wait(
            requested_checks, 
            timeout=self._poll_timeout * 2
        )

        completed, pending = check_tasks

        healthchecks: List[Call[HealthCheck]]  = await asyncio.gather(*completed)

        sorted_checks: List[Call[HealthCheck]] = list(sorted(
            healthchecks,
            key=lambda check: check[0]
        ))

        suspect = [
            (
                shard_id,
                check
            ) for shard_id, check in sorted_checks if check.status == 'suspect'
        ]

        healthy = [
            (
                shard_id,
                check
            ) for shard_id, check in sorted_checks if check.status == 'healthy' or check.status == 'investigating'
        ]

        suspect_checks: List[Call[HealthCheck]] = []
        for suspect_shard_id, suspect_check in suspect:

            newer_count = 0
            for healthy_shard_id, _ in healthy:
                if suspect_shard_id > healthy_shard_id:
                    newer_count += 1

            if newer_count >= len(healthy):
                suspect_checks.append((
                    suspect_shard_id,
                    suspect_check
                ))

        suspect_count = len(suspect_checks) + len(pending)
        
        await asyncio.gather(*[
            cancel(pending_check) for pending_check in pending
        ])

        return healthchecks, suspect_count

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def start_health_monitor(self):
        
        monitor_idx = 0

        while self._running:

            monitors = [
                address for address, monitoring_status in self._monitoring.items() if monitoring_status
            ]

            host: Union[str, None] = None
            port: Union[int, None] = None

            if len(monitors) > 0:
                monitor_idx = monitor_idx%len(monitors)
                host, port = monitors[monitor_idx]

            if self._monitoring.get((host, port)):
        
                self._active_checks_queue.append(
                    asyncio.create_task(
                        self._run_healthcheck(
                            host,
                            port
                        )
                    )
                )

            monitor_idx += 1

            await asyncio.sleep(self._poll_interval)      

    async def _start_suspect_monitor(self):

        elapsed = 0
        start = time.monotonic()

        if len(self._suspect_nodes) < 1:
            return
        
        address, suspect_shard_id = self._suspect_nodes.popitem()
        suspect_host, suspect_port = address
        
        if self._active_probes.get((suspect_host, suspect_port)):
            del self._active_probes[(suspect_host, suspect_port)]

        confirmation_task = self._failed_tasks.get((suspect_host, suspect_port))
        if confirmation_task:
            await cancel(confirmation_task)
            confirmation_task.cancel()

        node_revived = False

        while elapsed < self._max_suspect_timeout:
            
            confirmation_members = self._get_confirmation_members()

            healthchecks, suspect_count = await self._request_indirect_probe(
                suspect_host,
                suspect_port,
                confirmation_members
            )

            indirect_ack_count = len(self._investigating_nodes[(suspect_host, suspect_port)])

            suspect_count = len(self._investigating_nodes[(suspect_host, suspect_port)])

            if len(confirmation_members) > indirect_ack_count:
                # If we have received more results from other
                # nodes than nacks we may be a degraded node.
                self.status = 'degraded'

            elif suspect_count >= len(confirmation_members):
                # All members have confirmed suspect.

                self._suspect_nodes[(suspect_host, suspect_port)] = [
                    shard_id for shard_id, _ in healthchecks
                ]

                await asyncio.gather(*[
                    asyncio.wait_for(
                        self.submit_suspect_node(
                            node_host,
                            node_port,
                            suspect_host,
                            suspect_port,
                            self._suspect_nodes.get(
                                (suspect_host, suspect_port),
                                []
                            ),
                            self.status,
                            error_context=self.error_context
                        ),
                        timeout=self._poll_timeout
                    ) for node_host, node_port in confirmation_members
                ], return_exceptions=True)

            else:
                # We had a majority confirmation the node was healthy.

                self._monitoring[(suspect_host, suspect_port)] = True 

                monitoring = [
                    address for address, monitoring_status in self._monitoring.items() if monitoring_status
                ]

                for host, port in monitoring:
                    if self._suspect_nodes.get((host, port)) is None:
                        await self.submit_active_node(
                            host,
                            port,
                            suspect_host,
                            suspect_port,
                            health_status='healthy'
                        )

                await self.refresh_clients(
                    HealthCheck(
                        host=suspect_host,
                        port=suspect_port,
                        source_host=self.host,
                        source_port=self.port,
                        status='healthy'
                    )
                )

                await self._update_suspect_nodes(
                    confirmation_members,
                    self._investigating_nodes[(suspect_host, suspect_port)]
                )
                
                self._monitoring[(suspect_host, suspect_port)] = True

                node_revived = True
                break

            del self._investigating_nodes[(suspect_host, suspect_port)]

            await asyncio.sleep(self._poll_interval) 
            elapsed = time.monotonic() - start
        
        if node_revived is False:
            self._removed_nodes[(suspect_host, suspect_port)] = suspect_shard_id

        if self.status == 'investigating':
            self.status = 'healthy'

    async def cleanup_pending_checks(self):

        while self._running:

            for pending_check in list(self._active_checks_queue):
                if pending_check.done() or pending_check.cancelled():
                    self._active_checks_queue.remove(pending_check)

            monitoring = dict(self._monitoring)
            for host, port in self._removed_nodes:

                if monitoring.get((host, port)) is False:
                    await self.remove_clients(
                        HealthCheck(
                            host=host,
                            port=port,
                            source_host=self.host,
                            source_port=self.port,
                            status='removed'
                        )
                    )

                    del monitoring[(host, port)]

            self._removed_nodes.clear()
            self._monitoring = monitoring

            await asyncio.sleep(self._cleanup_interval)

    
    async def shutdown(self):
        self._running = False
        self._local_health_monitor.cancel()

        await asyncio.gather(*[
            cancel(remote_check) for remote_check in self._healthchecks.values()
        ])

        await cancel(self._local_health_monitor)
        
        await cancel(self._cleanup_task)

        await self.close()
            
            


    


