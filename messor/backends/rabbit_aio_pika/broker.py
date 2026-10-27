# coding: utf-8

import typing as t

from messor.core.broker import BaseBroker as _BaseBroker
from messor.backends.rabbit_aio_pika.consumer import Consumer
from messor.backends.rabbit_aio_pika.producer import Producer
from messor.backends.rabbit_aio_pika.resource import Resource

if t.TYPE_CHECKING:
    from messor.backends.rabbit_aio_pika.config import Config


class Broker(_BaseBroker["Config", bytes]):
    """RabbitMQ 通道。一条连接上分开发布和消费，再交给 Producer 和 Consumer。"""

    def __init__(self, config: "Config"):

        resource = Resource(
            serializer_cls=config.serializer_cls,
            name=config.name,
            url=config.url,
            queue_name=config.queue_name,
            enable_declare_queue=config.enable_declare_queue,
            queue_durable=config.queue_durable,
            queue_exclusive=config.queue_exclusive,
            queue_passive=config.queue_passive,
            queue_auto_delete=config.queue_auto_delete,
            queue_arguments=config.queue_arguments,
        )
        self._producer = Producer(
            resource=resource,
            publish_timeout=config.publish_timeout,
            exchange_name=config.exchange_name,
            routing_key=config.routing_key,
            delivery_mode=config.delivery_mode,
        )
        self._consumer = Consumer(
            resource=resource,
            prefetch_count=config.prefetch_count,
            prefetch_size=config.prefetch_size,
            global_qos=config.global_qos,
            coro=config.coro,
            consume_timeout=config.consume_timeout,
            pool_executor_cls=config.consumer_pool_executor_cls,
            pool_maxsize=config.consumer_pool_maxsize,
            rate_limit=config.rate_limit,
            rate_limiter=config.rate_limiter,
            rate_limit_storage=config.rate_limit_storage,
            rate_limit_key=config.rate_limit_key,
        )
