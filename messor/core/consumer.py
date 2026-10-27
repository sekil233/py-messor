# coding: utf-8

import abc
import asyncio
import logging
import typing as t

from messor.serializer.base import Message
from messor.core.resource import Resource
from messor.pool_executor.base import BasePoolExecutor
from messor.ratelimit.base import BaseRateLimiter
from messor.ratelimit.enums import (
    RateLimiter,
    RateLimitStorage,
    build_rate_limiter,
)
from messor.utils.hook import Hooks, HookOrder
from messor.utils.lazy import LazyObject


class BaseConsumer(t.Generic[Message, Resource], metaclass=abc.ABCMeta):

    __slots__ = (
        "_resource",
        "_coro",
        "_pool_executor",
        "_consume_timeout",
        "_on_coro_done",
        "_on_cleanup",
        "_stopped",
        "_startup",
        "_logger",
        "_task",
        "_rate_limiter",
    )

    def __init__(
            self,
            resource: Resource,
            coro: t.Callable[..., t.Coroutine],
            consume_timeout: float,
            pool_executor_cls: type[BasePoolExecutor],
            pool_maxsize: int,
            rate_limit: str | None = None,
            rate_limiter: RateLimiter = RateLimiter.TOKEN_BUCKET,
            rate_limit_storage: RateLimitStorage = RateLimitStorage.MEMORY,
            rate_limit_key: str | None = None):

        self._resource = resource
        self._coro = coro
        self._pool_executor = LazyObject[BasePoolExecutor](pool_executor_cls, pool_maxsize)
        self._consume_timeout = consume_timeout
        self._stopped = False
        self._on_coro_done = Hooks(order=HookOrder.LIFO)
        # 资源释放
        self._on_cleanup = Hooks(order=HookOrder.LIFO)
        self._startup = asyncio.Event()
        self._logger = logging.getLogger(__name__)
        self._task: asyncio.Task | None = None
        # 限流器：rate_limit 为空则不限流
        self._rate_limiter: BaseRateLimiter | None = None
        if rate_limit is not None:
            self._rate_limiter = build_rate_limiter(
                rate_limiter,
                rate_limit_storage,
                rate=rate_limit,
                key=rate_limit_key or self._resource.name,
            )

    async def consume(self) -> None:

        self._stopped = False
        # 未启动
        self._startup.clear()
        # 开启消费
        self._task = asyncio.create_task(self._consume_loop())
        # 等待启动完成
        await self._startup.wait()
        return None

    async def _on_startup(self) -> None:
        """启动执行"""

        pass

    async def _on_shutdown(self) -> None:
        """结束等待：self._drain之后触发"""

        await self._pool_executor.obj.join()

    async def _consume_loop(self) -> None:

        # 启动执行
        await self._on_startup()
        # 启动完成
        self._startup.set()
        while not self._stopped:
            try:
                await self._consume_once()
            except Exception as e:
                self._logger.exception(e)
                await asyncio.sleep(0.05)
        # 剩余处理
        await self._drain()
        # 结束等待
        await self._on_shutdown()
        # 资源回收
        await self._on_cleanup.run(reset=True)

    @abc.abstractmethod
    async def _consume_once(self) -> None:

        pass

    async def _dispatch(self, message: Message) -> None:
        """消息处理：先限流取名额，再提交给执行器"""

        if self._rate_limiter is not None:
            await self._rate_limiter.acquire()
        coro_args, coro_kwargs = self._resource.serializer.unpack(message)
        await self._pool_executor.obj.submit(self._run_coro, coro_args, coro_kwargs)

    async def _run_coro(self, *coro_args, **coro_kwargs) -> None:
        """执行用户协程，完成后触发 _on_coro_done 钩子（如 ack）"""

        try:
            await self._coro(*coro_args, **coro_kwargs) 
        finally:
            await self._on_coro_done.run(reset=False)

    async def _drain(self) -> None:
        """剩余处理"""

        pass

    async def stop(self) -> None:

        self._stopped = True

    async def join(self) -> None:
        
        if self._task is not None:
            await self._task
