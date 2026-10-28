# coding: utf-8

import asyncio

from messor.backends.queue.broker import Broker
from messor.backends.queue.config import Config
from messor.serializer.raw import Serializer

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
        name="q",
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
        Config(coro=noop, name="q", consume_timeout=0.05, maxsize=1),
    )

    channel = broker._producer._channel

    assert channel is broker._consumer._channel
    assert channel.serializer is Serializer


@run_async
async def test_broker_end_to_end_with_default_serializer():

    sums: list[int] = []

    async def add(a: int, b: int):
        sums.append(a + b)

    config = Config(
        coro=add,
        name="q",
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


@run_async
async def test_broker_rate_limit_slows_consumption():

    calls: list[float] = []

    async def work(value: int):
        # 记录完成时刻（monotonic），用于验证放行间隔
        calls.append(asyncio.get_running_loop().time())

    config = Config(
        coro=work,
        name="q",
        consume_timeout=0.05,
        consumer_pool_maxsize=10,
        maxsize=10,
        rate_limit="5/s",
    )
    broker = Broker(config)

    await broker.start_consumer()
    for i in range(10):
        await broker.publish((i,), {})
    await _wait_until(lambda: len(calls) == 10)

    await broker.stop_consumer()
    await broker.join_consumer()

    # 桶容量 5：前 5 条立即放行，后 5 条按 0.2s 间隔，
    # 首尾间隔应 >= 5 * 0.2 = 1.0s（留 0.1s 容差）
    assert len(calls) == 10
    assert calls[0] <= calls[-1]
    assert calls[-1] - calls[0] >= 0.9


@run_async
async def test_broker_rate_limit_key_uses_queue_name():

    async def noop():
        return None

    broker = Broker(
        Config(
            coro=noop,
            name="q",
            consume_timeout=0.05,
            maxsize=1,
            rate_limit="10/s",
        ),
    )

    assert broker._consumer._rate_limiter is not None
    assert broker._consumer._rate_limiter._key == "messor:rate:q"
