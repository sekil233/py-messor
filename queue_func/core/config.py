# coding: utf-8

import typing as t

from pydantic.config import ConfigDict
from pydantic.dataclasses import dataclass as pyd_dataclass

from queue_func.serializer.base import BaseSerializer
from queue_func.serializer.orjson_ser import Serializer
from queue_func.pool_executor.base import BasePoolExecutor
from queue_func.pool_executor.general import PoolExecutor


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

    ## consumer
    coro: t.Callable[[...], t.Coroutine]
    consume_timeout: float = 2.0
    consumer_pool_executor_cls: type[BasePoolExecutor] = PoolExecutor
    consumer_pool_maxsize: int = 200

    ## producer
    publish_timeout: float | None = None
