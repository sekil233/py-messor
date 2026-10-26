# coding: utf-8

import abc
import asyncio
import typing as t

from queue_func.core.resource import Resource
from queue_func.serializer.base import Message
from queue_func.utils.hook import Hooks, HookOrder
from queue_func.utils.waiter import IdleWaiter


class BaseProducer(t.Generic[Message, Resource], metaclass=abc.ABCMeta):

    __slots__ = (
        "_resource",
        "_publish_timeout",
        "_stopped",
        "_pub_waiter",
        "_on_cleanup",
        "_on_publish_success",
        "_lock",
        "_stop_task",
    )

    def __init__(
            self,
            resource: Resource,
            publish_timeout: float | None):

        self._resource = resource
        self._publish_timeout = publish_timeout
        self._stopped = False
        self._pub_waiter = IdleWaiter()
        self._stop_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

        ## push成功
        self._on_publish_success = Hooks(order=HookOrder.FIFO)

        ## 清理
        self._on_cleanup = Hooks(order=HookOrder.LIFO)

    async def publish(
            self,
            coro_args: tuple[t.Any],
            coro_kwargs: dict[str, t.Any] | None = None,
            *,
            timeout: float | int | None = None) -> None:

        if self._stopped:
            raise RuntimeError("publish called on already stopped")

        timeout = self._publish_timeout if timeout is None else timeout
        packed = self._resource.serializer.pack(coro_args, coro_kwargs)
        async with self._pub_waiter.hold():
            await self._publish(packed, timeout=timeout)
            await self._on_publish_success.run(reset=False)

    @abc.abstractmethod
    async def _publish(self, msg: Message, *, timeout: float | int | None) -> None:

        pass

    async def stop(self) -> None:

        if self._stopped:
            return None
        if self._stop_task is not None:
            return None

        self._stop_task = asyncio.create_task(self._stop_loop())
        return None

    async def _stop_loop(self) -> None:

        if self._stopped:
            return None

        # 确保push完成
        await self._pub_waiter.wait()
        await self._on_cleanup.run(reset=True)
        self._stopped = True
        return None

    async def join(self) -> None:

        if self._stop_task is not None:
            await self._stop_task
        return None
