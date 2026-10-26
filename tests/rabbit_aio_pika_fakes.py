# coding: utf-8

import asyncio
from contextlib import contextmanager
from unittest.mock import patch


class FakeIncoming:

    def __init__(self, body: bytes):

        self.body = body
        self.acked = False

    async def ack(self) -> None:

        self.acked = True


class FakeQueue:

    def __init__(self) -> None:

        self.messages: list[FakeIncoming] = []
        self.event = asyncio.Event()
        self.force_timeout = False

    def iterator(self, timeout: float | None = None):

        return FakeQueueIterator(self, timeout)


class FakeQueueIterator:

    def __init__(self, queue: FakeQueue, timeout: float | None):

        self._queue = queue
        self.timeout = timeout

    async def __aenter__(self):

        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:

        return None

    def __aiter__(self):

        return self

    async def __anext__(self) -> FakeIncoming:

        if self._queue.messages:
            return self._queue.messages.pop(0)
        if self._queue.force_timeout:
            raise asyncio.TimeoutError
        await self._queue.event.wait()
        self._queue.event.clear()
        if self._queue.messages:
            return self._queue.messages.pop(0)
        raise asyncio.TimeoutError


class FakeExchange:

    def __init__(self, world: "FakeWorld"):

        self.world = world
        self.published: list[tuple] = []
        self.block = False
        self.gate = asyncio.Event()
        self.raise_timeout = False

    async def publish(self, message, routing_key: str, timeout: float | None = None) -> None:

        self.published.append((message, routing_key, timeout))
        if self.block:
            await self.gate.wait()
        if self.raise_timeout:
            raise TimeoutError
        incoming = FakeIncoming(message.body)
        self.world.queue.messages.append(incoming)
        self.world.queue.event.set()


class FakeChannel:

    def __init__(self, world: "FakeWorld"):

        self.world = world
        self.default_exchange = FakeExchange(world)
        self.named_exchange: FakeExchange | None = None
        self.exchange_name: str | None = None
        self.declared: list[tuple[str, dict]] = []
        self.qos: dict | None = None
        self.closed = False

    async def declare_queue(self, name: str, **kwargs):

        self.declared.append((name, kwargs))
        return self.world.queue

    async def get_exchange(self, name: str) -> FakeExchange:

        self.exchange_name = name
        self.named_exchange = FakeExchange(self.world)
        return self.named_exchange

    async def set_qos(self, **kwargs) -> None:

        self.qos = kwargs

    async def close(self) -> None:

        self.closed = True


class FakeConnection:

    def __init__(self, world: "FakeWorld"):

        self.world = world
        self.channels: list[FakeChannel] = []
        self.closed = False

    async def channel(self) -> FakeChannel:

        channel = FakeChannel(self.world)
        self.channels.append(channel)
        return channel

    async def close(self) -> None:

        self.closed = True


class FakeWorld:

    def __init__(self) -> None:

        self.connection = FakeConnection(self)
        self.queue = FakeQueue()
        self.urls: list[str] = []

    async def connect_robust(self, url: str) -> FakeConnection:

        self.urls.append(url)
        self.connection.closed = False
        return self.connection


@contextmanager
def patch_connect(world: FakeWorld):

    with patch(
            "messor.backends.rabbit_aio_pika.resource.aio_pika.connect_robust",
            world.connect_robust,
    ):
        yield world


async def wait_until(predicate, *, timeout: float = 2.0, interval: float = 0.02) -> None:

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")
