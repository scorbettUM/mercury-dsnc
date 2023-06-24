import asyncio
import math
import random
import time
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
        self._check_nodes_count = env.MERCURY_SYNC_INDIRECT_CHECK_NODES

        self.min_suspect_multiplier = env.MERCURY_SYNC_MIN_SUSPECT_TIMEOUT_MULTIPLIER
        self.max_suspect_multiplier = env.MERCURY_SYNC_MAX_SUSPECT_TIMEOUT_MULTIPLIER 
        self._min_suspect_node_count = env.MERCURY_SYNC_MIN_SUSPECT_NODES_THRESHOLD
        self._local_health_multiplier = self.max_suspect_multiplier

        self._local_health_monitor: Optional[asyncio.Task] = None
        self._confirmed_suspicions: Dict[Tuple[str, int], int] = {}
        self._waiter: Optional[asyncio.Future] = None
        self._active_checks_queue: Deque[asyncio.Task] = deque()
        self._degraded_nodes: Deque[Tuple[str, int]] = deque()
        self._suspect_nodes: Deque[Tuple[str, int]] = deque()

        self._failed_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._suspect_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._investigating_nodes: Dict[Tuple[str, int], Dict[Tuple[str, int]]] = defaultdict(dict)
        self._node_statuses: Dict[Tuple[str, int], HealthStatus] = {}
        self._healthy_statuses = [
            'healthy'
        ]

        self._unhealthy_statuses = [
            'degraded',
            'suspect',
            'failed'
        ]

    @server()
    async def send_indirect_check(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        try:

            investigation_update = self._acknowledge_indirect_probe(
                source_host,
                source_port,
                target_host,
                target_port
            )


            self._node_statuses[(target_host, target_port)] = healthcheck.target_status

            indirect_probe = asyncio.wait_for(
                self.push_health_update(
                    target_host,
                    target_port,
                    self.status,
                    error_context=healthcheck.error
                ),
                timeout=self._poll_timeout * (self._local_health_multiplier + 1)
            )

            for task in asyncio.as_completed([
                investigation_update,
                indirect_probe
            ]):
                result: Union[Tuple[int, HealthCheck], None] = await task

                if isinstance(result, tuple):

                    _, response = result

                    self._local_health_multiplier = max(
                        0, 
                        self._local_health_multiplier - 1
                    )

                    self._node_statuses[(target_host, target_port)]  = response.status

                    # We've received a refutation
                    return HealthCheck(
                        host=healthcheck.source_host,
                        port=healthcheck.source_port,
                        target_status=response.status,   
                        source_host=response.source_host,
                        source_port=response.source_port,
                        status=response.status,
                        error=response.error
                    )

        except asyncio.TimeoutError:

            # Our suspicion is correct!
            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                source_host=target_host,
                source_port=target_port,
                target_status=healthcheck.status, 
                status=self.status
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

            self._node_statuses[(host, port)] =  'healthy'
            
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

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            error=self.error_context,
            status=self.status
        )
    
    @server()
    async def update_acknowledged(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port
        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        self._investigating_nodes[(target_host, target_port)].update({
            (source_host, source_port): healthcheck.status
        })

        return HealthCheck(
            host=source_host,
            port=source_port,
            source_host=self.host,
            source_port=self.port,
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

        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        if target_host and target_port: 
            self._node_statuses[(target_host, target_port)] = healthcheck.target_status

        local_node_status = self._node_statuses.get((source_host, source_port))

        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.pop((source_host, source_port), None)

        if suspect_task:
            await cancel(suspect_task)

            self._suspect_tasks = suspect_tasks

            
        if healthcheck.target_status == 'suspect' or healthcheck.target_status == 'degraded':

            self._local_health_multiplier = min(
                self._local_health_multiplier + 1,
                self.max_suspect_multiplier
            )

            monitoring = [
                address for address, status in self._node_statuses.items() if status in self._healthy_statuses
            ]

            # Send our refutation
            self._active_checks_queue.extend([
                asyncio.create_task(
                    self._run_healthcheck(
                        host,
                        port,
                        target_host=healthcheck.target_host,
                        target_port=healthcheck.target_port
                    )
                ) for host, port in monitoring
            ])

        status_unhealthy = local_node_status == 'failed' or local_node_status == 'suspect'
            
        if local_node_status is None or status_unhealthy:
            
            monitoring = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            await asyncio.gather(*[
                self.push_new_node(
                    host,
                    port,
                    source_host,
                    source_port,
                    healthcheck.status,
                    error_context=healthcheck.error
                ) for host, port in monitoring
            ])

            self._active_checks_queue.extend([
                asyncio.create_task(
                    self._run_healthcheck(
                        host,
                        port,
                    )
                ) for host, port in monitoring
            ])

        if local_node_status is None:

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

        elif status_unhealthy:
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

        is_buddy = target_host is None and target_port is None

        # Send our refutation
        if local_node_status == 'suspect' and healthcheck.status == 'healthy' and is_buddy:

            self._node_statuses[(source_host, source_port)] = 'healthy'
            source_address = (source_host, source_port)
            monitoring = [
                address for address, status in self._node_statuses.items() if status in self._healthy_statuses and address != source_address
            ]


            self._active_checks_queue.extend([
                asyncio.create_task(
                    self._run_healthcheck(
                        host,
                        port,
                        target_host=source_host,
                        target_port=source_port
                    )
                ) for host, port in monitoring if host != source_host and port != source_port
            ])

        self._node_statuses[(source_host, source_port)] = healthcheck.status

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
        
        target_status = self._node_statuses[(host, port)]
        if target_host and target_port:
            target_status = self._node_statuses[(target_host, target_port)]

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_status=target_status,
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
        
        target_status = self._node_statuses[(host, port)]
        if target_host and target_port:
            target_status = self._node_statuses[(target_host, target_port)]

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_status=target_status,
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
    
    @client('update_acknowledged')
    async def push_acknowledge_check(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        health_status: HealthStatus,
        timeout: float,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        
        await asyncio.sleep(0.8 * timeout)

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            status=health_status,
            error=error_context
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
            target_status=self._node_statuses[(target_host, target_port)],
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

        self._node_statuses[(host, port)] = 'healthy'

        self.status = 'healthy'
        self._running = True
        
        self._healthchecks[(host, port)] = asyncio.create_task(
            self.start_health_monitor()
        )
        
        self.confirmation_task = asyncio.create_task(
            self.cleanup_pending_checks()
        )

    def _calculate_min_suspect_timeout(self):
        nodes_count = len({
            address: status for address, status in self._node_statuses.items() if status != 'failed'
        }) + 1

        return round(
            self.min_suspect_multiplier * math.log10(nodes_count) * self._poll_interval,
            2
        )

    def _calculate_max_suspect_timeout(self, min_suspect_timeout: float):
        
        return round(
            self.max_suspect_multiplier * min_suspect_timeout,
            2
        )

    def _calculate_suspicion_timeout(
        self,
        suspect_node_address: Tuple[str, int]
    ):

        min_suspect_timeout = self._calculate_min_suspect_timeout()
        
        max_suspect_timeout = self._calculate_max_suspect_timeout(min_suspect_timeout)

        confirmed_suspect_count = max(
            0,
            self._confirmed_suspicions[suspect_node_address] - 1
        )

        timeout_modifier = math.log(
            confirmed_suspect_count + 1
        )/math.log(self._min_suspect_node_count + 1)

        timeout_difference = max_suspect_timeout - min_suspect_timeout

        return max(
            min_suspect_timeout,
            max_suspect_timeout - (timeout_difference * timeout_modifier)
        )
    
    async def _acknowledge_indirect_probe(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int
    ):
        try:
            
            timeout = self._poll_timeout * (self._local_health_multiplier + 1)
            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_acknowledge_check(
                    host,
                    port,
                    target_host,
                    target_port,
                    self.status,
                    timeout,
                    error_context=self.error_context
                ),
                timeout=timeout
            )

            _, healthcheck = response
            source_host, source_port = healthcheck.source_host, healthcheck.source_port

            self._node_statuses[(source_host, source_port)] = healthcheck.status

            self._local_health_multiplier = max(
                0, 
                self._local_health_multiplier - 1
            )

        except asyncio.TimeoutError:
            self._local_health_multiplier = min(
                self._local_health_multiplier + 1, 
                self.max_suspect_multiplier
            )

            if self._node_statuses[(host, port)] not in self._unhealthy_statuses:
                self._node_statuses[(host, port)] = 'degraded'

                self._degraded_nodes.append((
                    host,
                    port
                ))

                self._failed_tasks[(host, port)] = asyncio.create_task(
                    self._probe_timed_out_node()
                )

    async def _run_healthcheck(
        self, 
        host: str, 
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None
    ):

        try:

            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_health_update(
                    host,
                    port,
                    self.status,
                    target_host=target_host,
                    target_port=target_port,
                    error_context=self.error_context
                ),
                timeout=self._poll_timeout * (self._local_health_multiplier + 1)
            )

            _, healthcheck = response
            source_host, source_port = healthcheck.source_host, healthcheck.source_port

            self._node_statuses[(source_host, source_port)] = healthcheck.status

            self._local_health_multiplier = max(
                0, 
                self._local_health_multiplier - 1
            )

        except asyncio.TimeoutError:
            self._local_health_multiplier = min(
                self._local_health_multiplier + 1, 
                self.max_suspect_multiplier
            )

            if self._node_statuses[(host, port)] not in self._unhealthy_statuses:
                self._node_statuses[(host, port)] = 'degraded'

                self._degraded_nodes.append((
                    host,
                    port
                ))

                self._failed_tasks[(host, port)] = asyncio.create_task(
                    self._probe_timed_out_node()
                )

    async def _probe_timed_out_node(self):
        
        suspect_host, suspect_port = self._degraded_nodes.pop()
        
        confirmation_members = self._get_confirmation_members()

        healthchecks, suspect_count = await self._request_indirect_probe(
            suspect_host,
            suspect_port,
            confirmation_members
        )

        self._confirmed_suspicions[(suspect_host, suspect_port)] = len([
            check for _, check in healthchecks if check.target_status == 'suspect'
        ])

        indirect_ack_count = len(self._investigating_nodes[(suspect_host, suspect_port)])

        missing_ack_count = len(confirmation_members) - indirect_ack_count
        
        next_health_multiplier = self._local_health_multiplier + missing_ack_count - indirect_ack_count
        if next_health_multiplier < 0:
            self._local_health_multiplier = 0

        else:
            self._local_health_multiplier = min(
                next_health_multiplier, 
                self.max_suspect_multiplier
            )

        if suspect_count >= len(confirmation_members):

            self._node_statuses[(suspect_host, suspect_port)] = 'suspect'
            self._suspect_nodes.append((suspect_host, suspect_port))

            self._suspect_tasks[(suspect_host, suspect_port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )

        else:
            self._node_statuses[(suspect_host, suspect_port)] = 'healthy' 
            self.status = 'healthy'

        if self._investigating_nodes.get((suspect_host, suspect_port)):
            del self._investigating_nodes[(suspect_host, suspect_port)]
            
    def _get_confirmation_members(self) -> List[Tuple[str, int]]:
        confirmation_members = [
            address for address, status in self._node_statuses.items() if status in self._healthy_statuses
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
            timeout=self._poll_timeout * (self._local_health_multiplier + 1)
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
            ) for shard_id, check in sorted_checks if check.target_status == 'suspect'
        ]

        healthy = [
            (
                shard_id,
                check
            ) for shard_id, check in sorted_checks if check.target_status == 'healthy'
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
    
    async def _propagate_state_update(
        self,
        target_host: str,
        target_port: int
    ):
        monitoring = [
            address for address, status in self._node_statuses.items() if status in self._healthy_statuses
        ]

        for host, port in monitoring:
            await self.push_health_update(
                host,
                port,
                self.status,
                target_host=target_host,
                target_port=target_port
            )

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def start_health_monitor(self):
        
        monitor_idx = 0

        while self._running:

            monitors = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            host: Union[str, None] = None
            port: Union[int, None] = None

            if len(monitors) > 0:

                monitors_count = len(monitors)
                monitor_idx = monitor_idx%monitors_count
                host, port = monitors[monitor_idx]

            if self._node_statuses.get((host, port)):
        
                self._active_checks_queue.append(
                    asyncio.create_task(
                        self._run_healthcheck(
                            host,
                            port
                        )
                    )
                )

            if monitor_idx%monitors_count == 0:
                self.status = 'healthy'

            monitor_idx += 1

            await asyncio.sleep(
                self._poll_interval * (self._local_health_multiplier + 1)
            )      

    async def _start_suspect_monitor(self):

        elapsed = 0
        start = time.monotonic()

    
        if len(self._suspect_nodes) < 1:
            return
        
        address = self._suspect_nodes.pop()
        suspect_host, suspect_port = address

        confirmation_task = self._failed_tasks.get((suspect_host, suspect_port))
        if confirmation_task:
            await cancel(confirmation_task)
            confirmation_task.cancel()

        node_status = self._node_statuses[(suspect_host, suspect_port)]

        suspicion_timeout = self._calculate_suspicion_timeout(address)

        while elapsed < suspicion_timeout and node_status == 'suspect':
                
            confirmation_members = self._get_confirmation_members()

            healthchecks, suspect_count = await self._request_indirect_probe(
                suspect_host,
                suspect_port,
                confirmation_members
            )

            self._confirmed_suspicions[(suspect_host, suspect_port)] = len([
                check for _, check in healthchecks if check.target_status == 'suspect'
            ])

            indirect_ack_count = len(self._investigating_nodes[(suspect_host, suspect_port)])

            missing_ack_count = len(confirmation_members) - indirect_ack_count
            
            next_health_multiplier = self._local_health_multiplier + missing_ack_count - indirect_ack_count
            if next_health_multiplier < 0:
                self._local_health_multiplier = 0

            else:
                self._local_health_multiplier = min(
                    next_health_multiplier, 
                    self.max_suspect_multiplier
                )

            if suspect_count < len(confirmation_members):
                # We had a majority confirmation the node was healthy.

                self._node_statuses[(suspect_host, suspect_port)] = 'healthy'

                await self._propagate_state_update(
                    suspect_host,
                    suspect_port
                )

                await self.refresh_clients(
                    HealthCheck(
                        host=suspect_host,
                        port=suspect_port,
                        source_host=self.host,
                        source_port=self.port,
                        status=self.status
                    )
                )

            if self._investigating_nodes.get((suspect_host, suspect_port)):
                del self._investigating_nodes[(suspect_host, suspect_port)]

            await asyncio.sleep(
                self._poll_interval * (self._local_health_multiplier + 1)
            ) 

            elapsed = time.monotonic() - start
        
        if self._node_statuses[(suspect_host, suspect_port)] == 'suspect':
            self._node_statuses[(suspect_host, suspect_port)] = 'failed'
            await self._propagate_state_update(
                    suspect_host,
                    suspect_port
                )

    async def cleanup_pending_checks(self):

        while self._running:

            for pending_check in list(self._active_checks_queue):
                if pending_check.done() or pending_check.cancelled():
                    self._active_checks_queue.remove(pending_check)

            failed_nodes = [
                address for address, status in self._node_statuses.items() if status == 'failed'
            ]

            for host, port in failed_nodes:

                await self.remove_clients(
                    HealthCheck(
                        host=host,
                        port=port,
                        source_host=self.host,
                        source_port=self.port,
                        status='failed'
                    )
                )

            await asyncio.sleep(self._cleanup_interval)
    
    async def shutdown(self):
        self._running = False
        self._local_health_monitor.cancel()

        await asyncio.gather(*[
            cancel(check) for check in self._active_checks_queue
        ])

        await asyncio.gather(*[
            cancel(remote_check) for remote_check in self._healthchecks.values()
        ])

        await cancel(self._local_health_monitor)
        
        await cancel(self._cleanup_task)

        await self.close()
            
            


    


