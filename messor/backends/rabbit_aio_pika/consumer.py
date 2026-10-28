# coding: utf-8

import asyncio
import typing as t
from functools import partial

from messor.core.consumer import BaseConsumer as _BaseConsumer
from messor.pool_executor.base import BasePoolExecutor
from messor.ratelimit.enums import RateLimiter, RateLimitStorage
from messor.backends.rabbit_aio_pika.channel import Channel

if t.TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage, AbstractQueue


class Consumer(_BaseConsumer[bytes, Channel]):

    __slots__ = (
        "_prefetch_count",
        "_prefetch_size",
        "_global_qos",
        "_queue",
    )

    def __init__(
            self,
            channel: Channel,
            name: str,
            prefetch_count: int,
            prefetch_size: int,
            global_qos: bool,
            coro: t.Callable[..., t.Coroutine],
            consume_timeout: float,
            pool_executor_cls: type[BasePoolExecutor],
            pool_maxsize: int,
            rate_limit: str | None = None,
            rate_limiter: RateLimiter = RateLimiter.TOKEN_BUCKET,
            rate_limit_storage: RateLimitStorage = RateLimitStorage.MEMORY,
            rate_limit_key: str | None = None):

        super().__init__(
            channel=channel,
            name=name,
            coro=coro,
            consume_timeout=consume_timeout,
            pool_executor_cls=pool_executor_cls,
            pool_maxsize=pool_maxsize,
            rate_limit=rate_limit,
            rate_limiter=rate_limiter,
            rate_limit_storage=rate_limit_storage,
            rate_limit_key=rate_limit_key,
        )
        self._prefetch_count = prefetch_count
        self._prefetch_size = prefetch_size
        self._global_qos = global_qos
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

        self._on_cleanup.register(self._channel.try_close_connection)
        channel = await self._channel.get_channel()
        self._on_cleanup.register(partial(self._channel.close_channel, channel))
        await channel.set_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )
        queue = await self._channel.declare_queue(channel)
        self._queue = queue
        return queue
