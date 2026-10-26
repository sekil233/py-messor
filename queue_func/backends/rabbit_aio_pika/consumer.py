# coding: utf-8

import asyncio
import typing as t
from functools import partial

from queue_func.core.consumer import BaseConsumer as _BaseConsumer
from queue_func.pool_executor.base import BasePoolExecutor
from queue_func.backends.rabbit_aio_pika.resource import Resource

if t.TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage, AbstractQueue


class Consumer(_BaseConsumer[bytes, Resource]):

    __slots__ = (
        "_prefetch_count",
        "_prefetch_size",
        "_global_qos",
        # "_channel",
        "_queue",
    )

    def __init__(
            self,
            resource: Resource,
            prefetch_count: int,
            prefetch_size: int,
            global_qos: bool,
            coro: t.Callable[..., t.Coroutine],
            enable_consumer: bool,
            consume_timeout: float,
            pool_executor_cls: type[BasePoolExecutor],
            pool_maxsize: int):

        super().__init__(
            resource=resource,
            coro=coro,
            enable_consumer=enable_consumer,
            consume_timeout=consume_timeout,
            pool_executor_cls=pool_executor_cls,
            pool_maxsize=pool_maxsize,
        )
        self._prefetch_count = prefetch_count
        self._prefetch_size = prefetch_size
        self._global_qos = global_qos
        # self._channel: "AbstractChannel | None" = None
        self._queue: "AbstractQueue | None" = None

    async def _consume_once(self) -> None:

        queue = await self._ensure_queue()
        async with queue.iterator(timeout=self._consume_timeout) as queue_iter:
            message: "AbstractIncomingMessage"
            try:
                async for message in queue_iter:
                    body: bytes = message.body
                    await self._dispatch(body)
                    await message.ack()
            except asyncio.TimeoutError:
                pass

    async def _ensure_queue(self) -> "AbstractQueue":

        if self._queue is not None:
            return self._queue

        self._on_cleanup.register(self._resource.try_close_connection)
        channel = await self._resource.get_channel()
        self._on_cleanup.register(partial(self._resource.close_channel, channel))
        await channel.set_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )
        queue = await self._resource.declare_queue(channel)
        self._queue = queue
        return queue
