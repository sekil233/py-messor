# coding: utf-8

import pytest

from queue_func.errors import PublishTimeoutError
from queue_func.backends.queue.producer import Producer
from queue_func.serializer.raw import Serializer
from queue_func.backends.queue.resource import Resource

from tests.helpers import run_async


@run_async
async def test_producer_publish_puts_packed_message_on_queue():

    resource: Resource = Resource(Serializer, maxsize=2)
    producer = Producer(resource, publish_timeout=None)

    await producer.publish((1, 2), {"c": 3})

    wire = await resource.queue.get()
    assert Serializer.unpack(wire) == ((1, 2), {"c": 3})


@run_async
async def test_producer_publish_timeout_raises_when_queue_full():

    resource: Resource = Resource(Serializer, maxsize=1)
    await resource.queue.put(b"block")
    producer = Producer(resource, publish_timeout=0.05)

    with pytest.raises(PublishTimeoutError):
        await producer.publish((), {})

    assert issubclass(PublishTimeoutError, TimeoutError)


@run_async
async def test_producer_publish_call_timeout_overrides_config():

    resource: Resource = Resource(Serializer, maxsize=1)
    await resource.queue.put(b"block")
    producer = Producer(resource, publish_timeout=None)

    with pytest.raises(PublishTimeoutError):
        await producer.publish((), {}, timeout=0.05)


@run_async
async def test_producer_stop_rejects_later_publish_and_join_returns():

    resource: Resource = Resource(Serializer, maxsize=1)
    producer = Producer(resource, publish_timeout=None)

    await producer.stop()

    with pytest.raises(RuntimeError):
        await producer.publish((), {})

    await producer.join()
