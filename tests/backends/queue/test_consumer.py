# coding: utf-8

import asyncio

from queue_func.backends.queue.consumer import Consumer
from queue_func.pool_executor.general import PoolExecutor
from queue_func.serializer.raw import Serializer
from queue_func.backends.queue.resource import Resource

from tests.helpers import run_async


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> None:

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


def _resource(maxsize: int) -> Resource:

    return Resource(Serializer, maxsize=maxsize)


def _consumer(resource: Resource, handler, *, enabled: bool = True, pool_maxsize: int = 2) -> Consumer:

    return Consumer(
        resource=resource,
        consume_timeout=0.05,
        coro=handler,
        enable_consumer=enabled,
        pool_executor_cls=PoolExecutor,
        pool_maxsize=pool_maxsize,
    )


@run_async
async def test_consumer_runs_coro_with_unpacked_args():

    resource = _resource(2)
    received: list[tuple] = []

    async def handler(a, b, *, c):
        received.append((a, b, c))

    consumer = _consumer(resource, handler)

    await consumer.consume()
    await resource.queue.put(Serializer.pack((1, 2), {"c": 3}))
    await _wait_until(lambda: len(received) == 1)

    await consumer.stop()
    await consumer.join()

    assert received == [(1, 2, 3)]


@run_async
async def test_consumer_disabled_does_not_start_consume_loop():

    resource = _resource(1)

    async def handler():
        raise AssertionError("should not run")

    consumer = _consumer(resource, handler, enabled=False, pool_maxsize=1)

    await consumer.consume()
    assert consumer._worker is None
    assert consumer._pool_executor._obj is None


@run_async
async def test_consumer_drains_queue_after_stop():

    resource = _resource(2)
    received: list[int] = []

    async def handler(x):
        received.append(x)

    consumer = _consumer(resource, handler)

    await consumer.consume()
    await resource.queue.put(Serializer.pack((1,), {}))
    await resource.queue.put(Serializer.pack((2,), {}))
    await _wait_until(lambda: len(received) == 2)

    await consumer.stop()
    await consumer.join()

    assert sorted(received) == [1, 2]
