# coding: utf-8

from queue_func.backends.rabbit_aio_pika.broker import Broker
from queue_func.backends.rabbit_aio_pika.config import Config, DeliveryMode, ExchangeName
from queue_func.serializer.orjson_ser import Serializer

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect, wait_until


@run_async
async def test_broker_shares_one_connection_until_both_sides_release():

    calls: list[int] = []

    async def work(value: int):
        calls.append(value)

    config = Config(
        coro=work,
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="jobs",
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
        exchange_name=ExchangeName.DEFAULT,
        delivery_mode=DeliveryMode.PERSISTENT,
    )
    world = FakeWorld()
    broker = Broker(config)

    assert broker._producer._resource is broker._consumer._resource
    assert broker._producer._resource.serializer is Serializer

    with patch_connect(world):
        await broker.start_consumer()
        await broker.publish((7,), {})
        await wait_until(lambda: calls == [7])

        world.queue.force_timeout = True
        await broker.stop_consumer()
        world.queue.event.set()
        await broker.join_consumer()
        assert world.connection.closed is False

        await broker.stop_producer()
        await broker.join_producer()

    assert calls == [7]
    assert len(world.urls) == 1
    assert world.connection.closed is True
    assert all(channel.closed for channel in world.connection.channels)
