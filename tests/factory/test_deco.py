# coding: utf-8

import asyncio

import pytest

from queue_func.errors import PublishTimeoutError
from queue_func.factory import Backend, queue
from queue_func.factory.deco import QueueFunc

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
async def test_deco_push_and_consume_end_to_end():

    received: list[tuple[int, int]] = []

    @queue(
        Backend.QUEUE,
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
        maxsize=10,
    )
    async def add(a: int, b: int):
        received.append((a, b))

    assert isinstance(add, QueueFunc)

    await add.broker.start_consumer()
    await add.push(args=(3, 5))
    await _wait_until(lambda: received == [(3, 5)])

    await add.broker.stop_consumer()
    await add.broker.join_consumer()

    assert received == [(3, 5)]


@run_async
async def test_deco_default_queue_backend():

    hits: list[int] = []

    @queue(Backend.QUEUE, consume_timeout=0.05, maxsize=5)
    async def work(n: int):
        hits.append(n)

    await work.broker.start_consumer()
    await work.push(args=(9,))
    await _wait_until(lambda: hits == [9])

    await work.broker.stop_consumer()
    await work.broker.join_consumer()


@run_async
async def test_deco_wrapped_function_still_callable():

    @queue(Backend.QUEUE)
    async def double(x: int) -> int:
        return x * 2

    assert await double(4) == 8


@run_async
async def test_deco_push_accepts_kwargs():

    received: list[tuple] = []

    @queue(Backend.QUEUE, consume_timeout=0.05, maxsize=5)
    async def fn(a, b, *, c):
        received.append((a, b, c))

    await fn.broker.start_consumer()
    await fn.push(args=(1, 2), kwargs={"c": 3})
    await _wait_until(lambda: len(received) == 1)

    await fn.broker.stop_consumer()
    await fn.broker.join_consumer()

    assert received == [(1, 2, 3)]


@run_async
async def test_deco_push_timeout_overrides_config():

    @queue(Backend.QUEUE, maxsize=1, publish_timeout=None)
    async def work(n: int):
        return n

    await work.push(args=(1,))

    with pytest.raises(PublishTimeoutError):
        await work.push(args=(2,), timeout=0.05)
