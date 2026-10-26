# coding: utf-8

import asyncio

from queue_func.backends.queue.broker import Broker
from queue_func.backends.queue.config import Config
from queue_func.serializer.raw import Serializer

from tests.helpers import run_async


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> None:

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


@run_async
async def test_broker_wires_producer_to_consumer_end_to_end():

    calls: list[int] = []

    async def work(value: int):
        calls.append(value)

    config = Config(
        coro=work,
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
        maxsize=10,
    )
    broker = Broker(config)

    await broker.start_consumer()
    await broker.publish((7,), {})
    await _wait_until(lambda: calls == [7])

    await broker.stop_consumer()
    await broker.join_consumer()

    assert calls == [7]


@run_async
async def test_broker_producer_and_consumer_share_serializer():

    async def noop():
        return None

    broker = Broker(
        Config(coro=noop, consume_timeout=0.05, maxsize=1),
    )

    resource = broker._producer._resource

    assert resource is broker._consumer._resource
    assert resource.serializer is Serializer


@run_async
async def test_broker_end_to_end_with_default_serializer():

    sums: list[int] = []

    async def add(a: int, b: int):
        sums.append(a + b)

    config = Config(
        coro=add,
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
        maxsize=10,
    )
    broker = Broker(config)

    await broker.start_consumer()
    await broker.publish((3, 5), {})
    await _wait_until(lambda: sums == [8])

    await broker.stop_consumer()
    await broker.join_consumer()

    assert sums == [8]
