# coding: utf-8

import typing as t

import aio_pika

from messor.backends.rabbit_aio_pika.config import DeliveryMode, ExchangeName
from messor.errors import PublishTimeoutError
from messor.core.producer import BaseProducer
from messor.backends.rabbit_aio_pika.channel import Channel
from messor.utils.pool import BaseObjectPool, ObjectPool, StaticPool

if t.TYPE_CHECKING:
    from aio_pika.abc import (
        AbstractChannel as PikaAbstractChannel,
        AbstractExchange as PikaAbstractExchange,
    )


class _Slot(t.NamedTuple):
    """发布槽：channel 与其绑定的 exchange 成对池化"""

    channel: "PikaAbstractChannel"
    exchange: "PikaAbstractExchange"


class Producer(BaseProducer[bytes, Channel]):

    __slots__ = (
        "_exchange_name",
        "_routing_key",
        "_delivery_mode",
        "_pool",
    )

    def __init__(
            self,
            channel: Channel,
            publish_timeout: float | None,
            exchange_name: ExchangeName | str,
            routing_key: str,
            delivery_mode: DeliveryMode,
            producer_pool_maxsize: int | None = None):
        super().__init__(channel, publish_timeout)
        self._exchange_name = exchange_name
        self._routing_key = routing_key
        self._delivery_mode = delivery_mode
        # None 表示不池化：单槽常驻复用
        self._pool: BaseObjectPool[_Slot]
        if producer_pool_maxsize is None:
            self._pool = StaticPool(
                self._create_slot,
                closer=self._close_slot,
                healthy=self._slot_healthy)
        else:
            self._pool = ObjectPool(
                self._create_slot,
                producer_pool_maxsize,
                closer=self._close_slot,
                healthy=self._slot_healthy)
        # 清理顺序（LIFO）：先关槽（channel 计数归零），再关连接
        self._on_cleanup.register(self._channel.try_close_connection)
        self._on_cleanup.register(self._pool.close)

    async def _publish(self, msg: bytes, *, timeout: float | None) -> None:

        routing_key = self._routing_key or self._channel.queue_name
        message = aio_pika.Message(msg, delivery_mode=self._delivery_mode)

        slot = await self._pool.acquire()
        try:
            await slot.exchange.publish(message, routing_key=routing_key, timeout=timeout)
        except TimeoutError as exc:
            # confirm 超时后 channel 上可能有悬挂的 confirm，状态不可信，丢弃重建
            await self._pool.discard(slot)
            raise PublishTimeoutError("publish timed out") from exc
        except Exception:
            await self._pool.discard(slot)
            raise
        else:
            await self._pool.release(slot)

    async def _create_slot(self) -> _Slot:

        channel = await self._channel.get_channel()
        try:
            await self._channel.declare_queue(channel)
            if self._exchange_name == ExchangeName.DEFAULT:
                exchange = channel.default_exchange
            else:
                exchange = await channel.get_exchange(self._exchange_name)
        except Exception:
            # 失败必须归还 channel 计数，否则连接永远无法关闭
            await self._channel.close_channel(channel)
            raise
        return _Slot(channel, exchange)

    async def _close_slot(self, slot: _Slot) -> None:

        await self._channel.close_channel(slot.channel)

    @staticmethod
    def _slot_healthy(slot: _Slot) -> bool:

        return not slot.channel.is_closed
