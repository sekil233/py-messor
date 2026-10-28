# coding: utf-8

import asyncio
import typing as t

import aio_pika

from messor.core.channel import BaseChannel as _Base
from messor.serializer.base import Message, BaseSerializer

if t.TYPE_CHECKING:
    from aio_pika.abc import (
        AbstractChannel as PikaAbstractChannel,
        AbstractConnection as PikaAbstractConnection,
        AbstractQueue as PikaAbstractQueue,
    )


class PikaConnectionManager:
    """RabbitMQ 连接管理：连接生命周期与 channel 台账

    所有 channel 的开关都必须经过这里，保证 ``_active_channel_num``
    与连接状态一致。
    """

    __slots__ = (
        "_url",
        "_conn",
        "_lock",
        "_active_channel_num",
    )

    def __init__(self, url: str):

        self._url = url
        self._conn: "PikaAbstractConnection | None" = None
        self._lock = asyncio.Lock()
        self._active_channel_num = 0

    async def open_channel(self) -> "PikaAbstractChannel":
        """确保连接存在后开启 channel 并计数"""

        await self._ensure_conn()
        async with self._lock:
            channel = await self._required_conn.channel()
            self._active_channel_num += 1
            return channel

    async def close_channel(self, channel: "PikaAbstractChannel") -> None:

        async with self._lock:
            await channel.close()
            self._active_channel_num -= 1

    async def try_close_connection(self) -> None:
        """无活跃 channel 时关闭连接"""

        async with self._lock:
            if self._active_channel_num > 0 or self._conn is None:
                return None
            await self._required_conn.close()
            self._conn = None

    async def _ensure_conn(self) -> None:

        if self._conn is not None:
            return None
        async with self._lock:
            if self._conn is None:
                self._conn = await aio_pika.connect_robust(self._url)

    @property
    def _required_conn(self) -> "PikaAbstractConnection":

        if self._conn is None:
            raise RuntimeError("RabbitMQ connection not established")
        return self._conn


class Channel(_Base[Message]):
    """RabbitMQ 通道门面：组合连接管理与队列声明策略"""

    __slots__ = (
        "_manager",
        "_queue_name",
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
        self._manager = PikaConnectionManager(url)
        self._queue_name = queue_name
        self._enable_declare_queue = enable_declare_queue
        self._queue_durable = queue_durable
        self._queue_exclusive = queue_exclusive
        self._queue_passive = queue_passive
        self._queue_auto_delete = queue_auto_delete
        self._queue_arguments = queue_arguments

    @property
    def queue_name(self) -> str:

        return self._queue_name

    @property
    def manager(self) -> PikaConnectionManager:
        """连接管理器"""

        return self._manager

    async def declare_queue(self, channel: "PikaAbstractChannel") -> "PikaAbstractQueue":

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

    async def get_channel(self) -> "PikaAbstractChannel":

        return await self._manager.open_channel()

    async def close_channel(self, channel: "PikaAbstractChannel") -> None:

        await self._manager.close_channel(channel)

    async def try_close_connection(self) -> None:

        await self._manager.try_close_connection()
