# coding: utf-8

import asyncio
import typing as t

import aio_pika

from queue_func.utils.deco import before
from queue_func.core.resource import BaseResource as _Base
from queue_func.serializer.base import Message, BaseSerializer

if t.TYPE_CHECKING:
    from aio_pika.abc import AbstractConnection, AbstractChannel, AbstractQueue


class Resource(_Base[Message]):

    __slots__ = (
        "_queue_name",
        "_url",
        "_conn",
        "_lock",
        "_active_channel_num",
        "_enable_declare_queue",
        "_queue_durable",
        "_queue_exclusive",
        "_queue_passive",
        "_queue_auto_delete",
        "_queue_arguments",
    )

    def __init__(
            self,
            serializer_cls: type[BaseSerializer],
            url: str,
            queue_name: str,
            enable_declare_queue: bool,
            queue_durable: bool,
            queue_exclusive: bool,
            queue_passive: bool,
            queue_auto_delete: bool,
            queue_arguments: dict[str, t.Any] | None):

        super().__init__(serializer_cls)
        self._queue_name = queue_name
        self._url = url
        self._conn: "AbstractConnection | None" = None
        self._lock = asyncio.Lock()
        self._active_channel_num = 0
        self._enable_declare_queue = enable_declare_queue
        self._queue_durable = queue_durable
        self._queue_exclusive = queue_exclusive
        self._queue_passive = queue_passive
        self._queue_auto_delete = queue_auto_delete
        self._queue_arguments = queue_arguments

    @property
    def queue_name(self) -> str:

        return self._queue_name

    async def declare_queue(self, channel: "AbstractChannel") -> "AbstractQueue":

        if self._enable_declare_queue:
            return await channel.declare_queue(
                self._queue_name,
                durable=self._queue_durable,
                exclusive=self._queue_exclusive,
                passive=self._queue_passive,
                auto_delete=self._queue_auto_delete,
                arguments=self._queue_arguments,
            )
        return await channel.declare_queue(self._queue_name, passive=True)

    async def _ensure_conn(self) -> None:

        if self._conn is not None:
            return None

        async with self._lock:
            if self._conn is None:
                self._conn = await aio_pika.connect_robust(self._url)

    @property
    def _required_conn(self) -> "AbstractConnection":

        if self._conn is None:
            raise RuntimeError("RabbitMQ connection not established")
        return self._conn

    @before(_ensure_conn)
    async def get_channel(self) -> "AbstractChannel":

        async with self._lock:
            channel = await self._required_conn.channel()
            self._active_channel_num += 1
            return channel

    async def close_channel(self, channel: "AbstractChannel") -> None:

        async with self._lock:
            await channel.close()
            self._active_channel_num -= 1

    async def try_close_connection(self) -> None:

        async with self._lock:
            if self._active_channel_num > 0 or self._conn is None:
                return None
            await self._required_conn.close()
            self._conn = None
