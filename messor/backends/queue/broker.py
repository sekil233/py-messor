# coding: utf-8

import asyncio
import typing as t

from messor.serializer.base import Message
from messor.backends.queue.resource import Resource
from messor.core.broker import BaseBroker as _BaseBroker
from messor.backends.queue.producer import Producer
from messor.backends.queue.consumer import Consumer

if t.TYPE_CHECKING:
    from messor.backends.queue.config import Config


class Broker(_BaseBroker["Config", Message]):
    """内存队列通道。队列由这里创建，再交给 Producer 和 Consumer。"""

    IS_MEMORY = True

    def __init__(self, config: "Config"):
        resource = Resource(
            serializer_cls=config.serializer_cls,
            maxsize=config.maxsize)
        self._producer = Producer(
            resource=resource,
            publish_timeout=config.publish_timeout,
        )
        self._consumer = Consumer(
            resource=resource,
            coro=config.coro,
            consume_timeout=config.consume_timeout,
            pool_executor_cls=config.consumer_pool_executor_cls,
            pool_maxsize=config.consumer_pool_maxsize,
        )
