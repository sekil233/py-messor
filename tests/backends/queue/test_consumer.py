# coding: utf-8

import asyncio

from messor.backends.queue.consumer import Consumer
from messor.pool_executor.general import PoolExecutor
from messor.serializer.raw import Serializer
from messor.backends.queue.channel import Channel

from tests.helpers import run_async


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> None:

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


def _channel(maxsize: int) -> Channel:

    return Channel(Serializer, maxsize=maxsize)


def _consumer(channel: Channel, handler, *, pool_maxsize: int = 2) -> Consumer:

    return Consumer(
        channel=channel,
        name="q",
        consume_timeout=0.05,
        coro=handler,
        pool_executor_cls=PoolExecutor,
        pool_maxsize=pool_maxsize,
    )


@run_async
async def test_consumer_runs_coro_with_unpacked_args():

    channel = _channel(2)
    received: list[tuple] = []

    async def handler(a, b, *, c):
        received.append((a, b, c))

    consumer = _consumer(channel, handler)

    await consumer.consume()
    await channel.queue.put(Serializer.pack((1, 2), {"c": 3}))
    await _wait_until(lambda: len(received) == 1)

    await consumer.stop()
    await consumer.join()

    assert received == [(1, 2, 3)]


@run_async
async def test_consumer_drains_queue_after_stop():

    channel = _channel(2)
    received: list[int] = []

    async def handler(x):
        received.append(x)

    consumer = _consumer(channel, handler)

    await consumer.consume()
    await channel.queue.put(Serializer.pack((1,), {}))
    await channel.queue.put(Serializer.pack((2,), {}))
    await _wait_until(lambda: len(received) == 2)

    await consumer.stop()
    await consumer.join()

    assert sorted(received) == [1, 2]
