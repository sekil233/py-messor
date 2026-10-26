# coding: utf-8

import asyncio

import pytest

from messor.errors import PublishTimeoutError
from messor.factory import App, Backend
from messor.factory.deco import QueueFunc

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
async def test_app_queue_push_and_consume_end_to_end():

    received: list[tuple[int, int]] = []
    app = App()

    @app.queue(
        Backend.QUEUE,
        name="add",
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
        maxsize=10,
    )
    async def add(a: int, b: int):
        received.append((a, b))

    assert isinstance(add, QueueFunc)

    await app.start()
    await add.push(args=(3, 5))
    await _wait_until(lambda: received == [(3, 5)])

    await app.stop()

    assert received == [(3, 5)]


@run_async
async def test_app_queue_default_backend():

    hits: list[int] = []
    app = App()

    @app.queue(Backend.QUEUE, name="work", consume_timeout=0.05, maxsize=5)
    async def work(n: int):
        hits.append(n)

    await app.start()
    await work.push(args=(9,))
    await _wait_until(lambda: hits == [9])

    await app.stop()


@run_async
async def test_app_queue_wrapped_function_still_callable():

    app = App()

    @app.queue(Backend.QUEUE, name="double")
    async def double(x: int) -> int:
        return x * 2

    assert await double(4) == 8


@run_async
async def test_app_queue_push_accepts_kwargs():

    received: list[tuple] = []
    app = App()

    @app.queue(Backend.QUEUE, name="fn", consume_timeout=0.05, maxsize=5)
    async def fn(a, b, *, c):
        received.append((a, b, c))

    await app.start()
    await fn.push(args=(1, 2), kwargs={"c": 3})
    await _wait_until(lambda: len(received) == 1)

    await app.stop()

    assert received == [(1, 2, 3)]


@run_async
async def test_app_queue_push_timeout_overrides_config():

    app = App()

    @app.queue(Backend.QUEUE, name="work", maxsize=1, publish_timeout=None)
    async def work(n: int):
        return n

    await work.push(args=(1,))

    with pytest.raises(PublishTimeoutError):
        await work.push(args=(2,), timeout=0.05)


def test_app_queue_name_required():

    app = App()

    with pytest.raises(TypeError):
        @app.queue(Backend.QUEUE)
        async def work():
            return None


def test_app_queue_name_unique():

    app = App()

    @app.queue(Backend.QUEUE, name="work")
    async def work():
        return None

    with pytest.raises(ValueError):
        @app.queue(Backend.QUEUE, name="work")
        async def other():
            return None
