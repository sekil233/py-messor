# coding: utf-8

import asyncio

import pytest

from queue_func.backends.rabbit_aio_pika.config import DeliveryMode, ExchangeName
from queue_func.errors import PublishTimeoutError
from queue_func.backends.rabbit_aio_pika.producer import Producer
from queue_func.serializer.orjson_ser import Serializer
from queue_func.backends.rabbit_aio_pika.resource import Resource

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect, wait_until


def _resource(world: FakeWorld) -> Resource:

    return Resource(
        serializer_cls=Serializer,
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="jobs",
        enable_declare_queue=False,
        queue_durable=False,
        queue_exclusive=False,
        queue_passive=False,
        queue_auto_delete=False,
        queue_arguments=None,
    )


def _producer(resource: Resource, **kwargs) -> Producer:

    values = dict(
        resource=resource,
        publish_timeout=None,
        exchange_name=ExchangeName.DEFAULT,
        routing_key="",
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    values.update(kwargs)
    return Producer(**values)


@run_async
async def test_publish_uses_default_exchange_and_queue_name():

    world = FakeWorld()
    resource = _resource(world)
    producer = _producer(resource)

    with patch_connect(world):
        await producer.publish((1, 2), {"c": 3})

    channel = world.connection.channels[0]
    message, routing_key, timeout = channel.default_exchange.published[0]
    assert routing_key == "jobs"
    assert timeout is None
    assert int(message.delivery_mode) == DeliveryMode.PERSISTENT
    assert Serializer.unpack(message.body) == ((1, 2), {"c": 3})
    assert channel.exchange_name is None
    assert len(world.connection.channels) == 1


@run_async
async def test_publish_uses_named_exchange_and_routing_key():

    world = FakeWorld()
    resource = _resource(world)
    producer = _producer(
        resource,
        exchange_name=ExchangeName.TOPIC,
        routing_key="rk",
        delivery_mode=DeliveryMode.NOT_PERSISTENT,
    )

    with patch_connect(world):
        await producer.publish((1,), {}, timeout=0.2)

    channel = world.connection.channels[0]
    assert channel.exchange_name == ExchangeName.TOPIC
    assert channel.default_exchange.published == []
    message, routing_key, timeout = channel.named_exchange.published[0]
    assert routing_key == "rk"
    assert timeout == 0.2
    assert int(message.delivery_mode) == DeliveryMode.NOT_PERSISTENT


@run_async
async def test_publish_timeout_raises_publish_timeout_error():

    world = FakeWorld()
    resource = _resource(world)
    producer = _producer(resource)

    with patch_connect(world):
        channel_holder: list = []

        original = world.connection.channel

        async def channel():
            created = await original()
            created.default_exchange.raise_timeout = True
            channel_holder.append(created)
            return created

        world.connection.channel = channel
        with pytest.raises(PublishTimeoutError):
            await producer.publish((), {})

    assert channel_holder[0].default_exchange.published


@run_async
async def test_stop_waits_for_inflight_publish_then_closes():

    world = FakeWorld()
    resource = _resource(world)
    producer = _producer(resource)

    with patch_connect(world):
        world_exchange_block = asyncio.Event()

        original = world.connection.channel

        async def channel():
            created = await original()
            created.default_exchange.block = True
            created.default_exchange.gate = world_exchange_block
            return created

        world.connection.channel = channel
        publish_task = asyncio.create_task(producer.publish((4,), {}))
        await wait_until(lambda: world.connection.channels and world.connection.channels[0].default_exchange.published)

        async def shutdown():
            await producer.stop()
            await producer.join()

        shutdown_task = asyncio.create_task(shutdown())
        await wait_until(lambda: producer._stopped)
        world_exchange_block.set()
        await publish_task
        await shutdown_task

    channel = world.connection.channels[0]
    assert channel.closed is True
    assert world.connection.closed is True
    assert producer._channel is None


@run_async
async def test_publish_after_stop_raises():

    world = FakeWorld()
    producer = _producer(_resource(world))

    with patch_connect(world):
        await producer.stop()
        with pytest.raises(RuntimeError):
            await producer.publish((), {})
        await producer.join()

    assert world.urls == []
