from messor.serializer.base import Message
from messor.core.config import Config
from messor.core.broker import BaseBroker

from tests.helpers import run_async


class _Broker(BaseBroker[Config, Message]):

    pass


class _FakeProducer:

    def __init__(self):
        self.published: list[tuple] = []
        self.stopped = 0
        self.joined = 0

    async def publish(self, coro_args, coro_kwargs=None, *, timeout=None):
        self.published.append((coro_args, coro_kwargs, timeout))

    async def stop(self):
        self.stopped += 1

    async def join(self):
        self.joined += 1


class _FakeConsumer:

    def __init__(self):
        self.consume_count = 0
        self.stopped = 0
        self.joined = 0

    async def consume(self):
        self.consume_count += 1

    async def stop(self):
        self.stopped += 1

    async def join(self):
        self.joined += 1


def _broker() -> _Broker:

    broker = _Broker()
    broker._producer = _FakeProducer()
    broker._consumer = _FakeConsumer()
    return broker


@run_async
async def test_broker_publish_delegates_to_producer():

    broker = _broker()

    await broker.publish((1, 2), {"c": 3}, timeout=0.5)

    assert broker._producer.published == [((1, 2), {"c": 3}, 0.5)]


@run_async
async def test_broker_publish_without_kwargs():

    broker = _broker()

    await broker.publish((7,))

    assert broker._producer.published == [((7,), None, None)]


@run_async
async def test_broker_consumer_lifecycle_delegates():

    broker = _broker()

    await broker.start_consumer()
    await broker.stop_consumer()
    await broker.join_consumer()

    assert broker._consumer.consume_count == 1
    assert broker._consumer.stopped == 1
    assert broker._consumer.joined == 1


@run_async
async def test_broker_producer_lifecycle_delegates():

    broker = _broker()

    await broker.stop_producer()
    await broker.join_producer()

    assert broker._producer.stopped == 1
    assert broker._producer.joined == 1
