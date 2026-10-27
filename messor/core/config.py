# coding: utf-8

import typing as t

from pydantic.config import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass

from messor.serializer.base import BaseSerializer
from messor.serializer.orjson_ser import Serializer
from messor.pool_executor.base import BasePoolExecutor
from messor.pool_executor.general import PoolExecutor
from messor.ratelimit.enums import RateLimiter, RateLimitStorage


ExtraIgnoreConfig = ConfigDict(
    extra="ignore",
    validate_assignment=True,
    arbitrary_types_allowed=True,
    coerce_numbers_to_str=True,
)


@pyd_dataclass(frozen=True, kw_only=True, config=ExtraIgnoreConfig)
class Config:

    ## shared resource
    serializer_cls: type[BaseSerializer] = Serializer

    # 队列逻辑名，每个队列唯一
    name: str

    ## consumer
    coro: t.Callable[[...], t.Coroutine]
    consume_timeout: float = 2.0
    consumer_pool_executor_cls: type[BasePoolExecutor] = PoolExecutor
    consumer_pool_maxsize: int = 200

    ## consumer rate limit
    # 消费速率，如 "100/s"、"50/m"、"10/h"、"5/d"；None 表示不限流
    rate_limit: str | None = None
    rate_limiter: RateLimiter = RateLimiter.TOKEN_BUCKET
    rate_limit_storage: RateLimitStorage = RateLimitStorage.MEMORY
    # 限流桶 key；None 时使用队列逻辑名 name
    rate_limit_key: str | None = None

    ## producer
    publish_timeout: float | None = None
