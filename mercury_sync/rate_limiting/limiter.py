import asyncio
from mercury_sync.env import Env
from mercury_sync.models.limit import Limit
from mercury_sync.models.request import Request
from pydantic import IPvAnyAddress
from typing import (
    Optional,
    Callable,
    Dict,
    Tuple,
    Union
)
from .limiters import (
    LeakyBucketLimiter,
    SlidingWindowLimiter,
    TokenBucketLimiter
)

class Limiter:

    def __init__(
        self,
        env: Env
    ) -> None:
        
        self._limiter: Union[TokenBucketLimiter, None] = None

        self._default_limit = Limit(
            request_limit=env.MERCURY_SYNC_HTTP_RATE_LIMIT_REQUESTS,
            request_period=env.MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD
        )
        
        self._rate_limit_strategy = env.MERCURY_SYNC_HTTP_RATE_LIMIT_STRATEGY
        self._default_limiter_type = env.MERCURY_SYNC_HTTP_RATE_LIMITER_TYPE
        
        self._rate_limiter_types: Dict[
            str,
            Callable[
                [int, str],
                Union[
                    LeakyBucketLimiter,
                    SlidingWindowLimiter,
                    TokenBucketLimiter
                ]
            ]
        ] = {
            "leaky-bucket": lambda max_concurrency, rate_limit_period: LeakyBucketLimiter(
                max_concurrency,
                rate_limit_period
            ),
            "sliding-window":  lambda max_concurrency, rate_limit_period: SlidingWindowLimiter(
                max_concurrency,
                rate_limit_period
            ),
            "token-bucket":  lambda max_concurrency, rate_limit_period: TokenBucketLimiter(
                max_concurrency,
                rate_limit_period
            ),
        }

        self._rate_limit_period = env.MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD

        self._rate_limiters: Dict[
            str, 
            Union[
                LeakyBucketLimiter,
                SlidingWindowLimiter,
                TokenBucketLimiter
            ]
        ] = {}

    async def limit(
        self,
        ip_address: IPvAnyAddress,
        request: Request,
        limit: Optional[Limit]=None
    ):
        limit_key: Union[str, None] = None

        if self._rate_limit_strategy == 'ip':

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=ip_address
            )

        elif self._rate_limit_strategy == 'endpoint' and limit:

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=request.path
            )
        
        elif self._rate_limit_strategy == 'global':

            limit_key = limit.get_key(
                request,
                ip_address,
                default='default'
            )

        elif self._rate_limit_strategy == "ip-endpoint" and limit:

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=f'{request.path}_{ip_address}'
            )

        elif limit:

            limit_key = limit.get_key(
                request,
                ip_address
            )

        if limit_key and limit.matches(request, ip_address):
            await self._check_limiter(
                limit_key,
                limit
            ) 


    async def _check_limiter(
        self,
        limiter_key: str,
        limit: Limit
    ):
        
        limiter = self._rate_limiters.get(limiter_key)

        rate_limiter_type = limit.limiter_type
        if rate_limiter_type is None:
            rate_limiter_type = self._default_limiter_type

        if limiter is None:

            limiter = self._rate_limiter_types.get(rate_limiter_type)(
                limit.request_limit,
                limit.period()
            )

            self._rate_limiters[limiter_key] = limiter
            

        await limiter.acquire()