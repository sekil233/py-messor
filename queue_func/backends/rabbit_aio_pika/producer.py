# coding: utf-8

import asyncio
import typing as t
from functools import partial

import aio_pika

from queue_func.backends.rabbit_aio_pika.config import DeliveryMode, ExchangeName
from queue_func.errors import PublishTimeoutError
from queue_func.core.producer import BaseProducer
from queue_func.backends.rabbit_aio_pika.resource import Resource

if t.TYPE_CHECKING:
    from aio_pika.abc import AbstractExchange


class Producer(BaseProducer[bytes, Resource]):

    __slots__ = (
        "_exchange_name",
        "_exchange_type",
        "_routing_key",
        "_delivery_mode",
        "_exchange",
    )

    def __init__(
            self,
            resource: Resource,
            publish_timeout: float | None,
            exchange_name: ExchangeName | str,
            routing_key: str,
            delivery_mode: DeliveryMode):
        super().__init__(resource, publish_timeout)
        self._exchange_name = exchange_name
        self._exchange: "AbstractExchange | None" = None
        self._routing_key = routing_key
        self._delivery_mode = delivery_mode

    async def _publish(self, msg: bytes, *, timeout: float | None) -> None:

        routing_key = self._routing_key or self._resource.queue_name
        message = aio_pika.Message(msg, delivery_mode=self._delivery_mode)

        await self._ensure_exchange()
        try:
            await self._exchange.publish(message, routing_key=routing_key, timeout=timeout)
        except TimeoutError as exc:
            raise PublishTimeoutError("publish timed out") from exc

    async def _ensure_exchange(self) -> "AbstractExchange":

        if self._exchange is not None:
            return self._exchange

        async with self._lock:
            if self._exchange is None:
                self._on_cleanup.register(self._resource.try_close_connection)
                channel = await self._resource.get_channel()
                self._on_cleanup.register(partial(self._resource.close_channel, channel))
                await self._resource.declare_queue(channel)
                if self._exchange_name == ExchangeName.DEFAULT:
                    exchange = channel.default_exchange
                else:
                    exchange = await channel.get_exchange(self._exchange_name)
                self._exchange = exchange
                return exchange
