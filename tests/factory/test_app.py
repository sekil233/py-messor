# coding: utf-8

import asyncio

import pytest

from messor.factory import App, Backend
from messor.factory.deco import QueueFunc

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect, wait_until


async def _wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> None:

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


@run_async
async def test_app_start_memory_consumers_reverse_order(monkeypatch):

    from messor.backends.queue.broker import Broker

    app = App()
    order: list[str] = []

    @app.queue(Backend.QUEUE, name="a", consume_timeout=0.05, maxsize=10)
    async def task_a():
        return None

    @app.queue(Backend.QUEUE, name="b", consume_timeout=0.05, maxsize=10)
    async def task_b():
        return None

    orig = Broker.start_consumer

    async def spy(self):
        order.append(self._consumer._coro.__name__)
        await orig(self)

    monkeypatch.setattr(Broker, "start_consumer", spy)

    await app.start()

    assert order == ["task_b", "task_a"]

    await app.stop()


@run_async
async def test_app_start_rabbit_consumers_reverse_order(monkeypatch):

    from messor.backends.rabbit_aio_pika.broker import Broker

    app = App()
    order: list[str] = []

    @app.queue(
        Backend.RABBIT_AIO_PIKA,
        name="r1",
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="q1",
        enable_declare_queue=False,
        consume_timeout=0.05,
        consumer_pool_maxsize=1,
    )
    async def task_r1():
        return None

    @app.queue(
        Backend.RABBIT_AIO_PIKA,
        name="r2",
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="q2",
        enable_declare_queue=False,
        consume_timeout=0.05,
        consumer_pool_maxsize=1,
    )
    async def task_r2():
        return None

    orig = Broker.start_consumer

    async def spy(self):
        order.append(self._consumer._coro.__name__)
        await orig(self)

    monkeypatch.setattr(Broker, "start_consumer", spy)

    world = FakeWorld()
    with patch_connect(world):
        await app.start()

        assert order == ["task_r2", "task_r1"]

        world.queue.force_timeout = True
        world.queue.event.set()
        await app.stop()

    assert world.connection.closed is True


@run_async
async def test_app_start_unknown_name_raises():

    app = App()

    @app.queue(Backend.QUEUE, name="a", consume_timeout=0.05)
    async def task_a():
        return None

    with pytest.raises(ValueError):
        await app.start(queues=["nope"])


@run_async
async def test_app_start_memory_must_be_included():

    app = App()

    @app.queue(Backend.QUEUE, name="a", consume_timeout=0.05)
    async def task_a():
        return None

    @app.queue(Backend.QUEUE, name="b", consume_timeout=0.05)
    async def task_b():
        return None

    with pytest.raises(ValueError):
        await app.start(queues=["a"])


@run_async
async def test_app_start_selective_allows_memory_without_rabbit():

    app = App()

    @app.queue(Backend.QUEUE, name="m", consume_timeout=0.05, maxsize=5)
    async def task_m():
        return None

    @app.queue(
        Backend.RABBIT_AIO_PIKA,
        name="r",
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="q",
        enable_declare_queue=False,
        consume_timeout=0.05,
        consumer_pool_maxsize=1,
    )
    async def task_r():
        return None

    world = FakeWorld()
    with patch_connect(world):
        await app.start(queues=["m"])

        # 只启动了内存队列，rabbit 未启动
        assert task_r.broker._consumer._task is None

        await app.stop()

    # 未启动的 rabbit producer 也会被 stop/join
    assert task_r.broker._producer._stopped is True


@run_async
async def test_app_stop_drains_memory_after_start():

    hits_a: list[int] = []
    hits_b: list[int] = []
    app = App()

    @app.queue(Backend.QUEUE, name="a", consume_timeout=0.05, maxsize=10)
    async def task_a(n: int):
        hits_a.append(n)

    @app.queue(Backend.QUEUE, name="b", consume_timeout=0.05, maxsize=10)
    async def task_b(n: int):
        hits_b.append(n)

    await app.start()
    await task_a.push(args=(1,))
    await task_b.push(args=(2,))
    await _wait_until(lambda: hits_a == [1] and hits_b == [2])

    await app.stop()

    assert hits_a == [1]
    assert hits_b == [2]
