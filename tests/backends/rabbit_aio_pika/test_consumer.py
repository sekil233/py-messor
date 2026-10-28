# coding: utf-8

from messor.backends.rabbit_aio_pika.consumer import Consumer
from messor.pool_executor.general import PoolExecutor
from messor.serializer.orjson_ser import Serializer
from messor.backends.rabbit_aio_pika.channel import Channel

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeIncoming, FakeWorld, patch_connect, wait_until


def _channel(world: FakeWorld) -> Channel:

    return Channel(
        serializer_cls=Serializer,
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="jobs",
        enable_declare_queue=False,
        queue_durable=False,
        queue_exclusive=False,
        queue_passive=False,
        queue_auto_delete=False,
        queue_arguments=None,
    )


def _consumer(channel: Channel, handler) -> Consumer:

    return Consumer(
        channel=channel,
        name="q",
        prefetch_count=3,
        prefetch_size=0,
        global_qos=False,
        coro=handler,
        consume_timeout=0.05,
        pool_executor_cls=PoolExecutor,
        pool_maxsize=2,
    )


@run_async
async def test_consume_unpacks_and_acks():

    world = FakeWorld()
    received: list[tuple] = []

    async def handler(a, b, *, c):
        received.append((a, b, c))

    consumer = _consumer(_channel(world), handler)
    message = FakeIncoming(Serializer.pack((1, 2), {"c": 3}))

    with patch_connect(world):
        await consumer.consume()
        world.queue.messages.append(message)
        world.queue.event.set()
        await wait_until(lambda: received == [(1, 2, 3)] and message.acked)

        world.queue.force_timeout = True
        await consumer.stop()
        world.queue.event.set()
        await consumer.join()

    channel = world.connection.channels[0]
    assert channel.qos == {"prefetch_count": 3, "prefetch_size": 0, "global_": False}
    assert channel.declared == [("jobs", {"passive": True})]
    assert channel.closed is True
    assert world.connection.closed is True
