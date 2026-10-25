import asyncio

import pytest

from queue_func.serializer.base import Message
from queue_func.serializer.raw import Serializer
from queue_func.core.resource import BaseResource
from queue_func.core.producer import BaseProducer

from tests.helpers import run_async


class _Producer(BaseProducer[Message, BaseResource]):

    def __init__(self, resource: BaseResource, publish_timeout: float | None):
        self.published: list[tuple] = []
        super().__init__(resource, publish_timeout)

    async def _publish(self, msg: Message, *, timeout: float | None) -> None:
        self.published.append((msg, timeout))


def _resource() -> BaseResource:

    return BaseResource(Serializer)


@run_async
async def test_publish_packs_and_uses_config_timeout():

    producer = _Producer(_resource(), publish_timeout=1.5)

    await producer.publish((1, 2), {"c": 3})

    assert producer.published == [(((1, 2), {"c": 3}), 1.5)]

    await producer.stop()
    await producer.join()


@run_async
async def test_publish_call_timeout_overrides_config():

    producer = _Producer(_resource(), publish_timeout=1.5)

    await producer.publish((1, 2), {"c": 3}, timeout=0.25)

    assert producer.published == [(((1, 2), {"c": 3}), 0.25)]

    await producer.stop()
    await producer.join()


@run_async
async def test_publish_after_stop_raises_runtime_error():

    producer = _Producer(_resource(), publish_timeout=None)

    await producer.stop()

    with pytest.raises(RuntimeError):
        await producer.publish((), {})

    await producer.join()


@run_async
async def test_stop_waits_for_in_flight_publish():

    gate = asyncio.Event()

    class _BlockingProducer(_Producer):

        async def _publish(self, msg: Message, *, timeout: float | None) -> None:
            await gate.wait()

    producer = _BlockingProducer(_resource(), publish_timeout=None)

    publishing = asyncio.create_task(producer.publish((), {}))
    await asyncio.sleep(0.01)

    await producer.stop()
    joined = asyncio.create_task(producer.join())
    await asyncio.sleep(0.01)
    assert not joined.done()

    gate.set()
    await joined
    await publishing
