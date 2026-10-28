# coding: utf-8

from messor.serializer.orjson_ser import Serializer
from messor.backends.rabbit_aio_pika.channel import Channel

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect


def _channel(world: FakeWorld, **kwargs) -> Channel:

    values = dict(
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
    values.update(kwargs)
    return Channel(**values)


@run_async
async def test_declare_queue_is_passive_when_declare_disabled():

    world = FakeWorld()
    channel = _channel(world)

    with patch_connect(world):
        aio_channel = await channel.get_channel()
        queue = await channel.declare_queue(aio_channel)

    assert channel.queue_name == "jobs"
    assert queue is world.queue
    assert aio_channel.declared == [("jobs", {"passive": True})]
    assert world.urls == ["amqp://guest:guest@127.0.0.1/"]


@run_async
async def test_declare_queue_forwards_declare_flags():

    world = FakeWorld()
    channel = _channel(
        world,
        enable_declare_queue=True,
        queue_durable=True,
        queue_exclusive=True,
        queue_passive=True,
        queue_auto_delete=True,
        queue_arguments={"x-message-ttl": 1000},
    )

    with patch_connect(world):
        aio_channel = await channel.get_channel()
        await channel.declare_queue(aio_channel)

    assert aio_channel.declared == [(
        "jobs",
        {
            "durable": True,
            "exclusive": True,
            "passive": True,
            "auto_delete": True,
            "arguments": {"x-message-ttl": 1000},
        },
    )]


@run_async
async def test_connection_closes_only_after_the_last_channel():

    world = FakeWorld()
    channel = _channel(world)

    with patch_connect(world):
        first = await channel.get_channel()
        second = await channel.get_channel()

        assert len(world.urls) == 1
        assert channel._active_channel_num == 2

        await channel.close_channel(first)
        await channel.try_close_connection()
        assert world.connection.closed is False

        await channel.close_channel(second)
        await channel.try_close_connection()

    assert first.closed is True
    assert second.closed is True
    assert world.connection.closed is True
    assert channel._active_channel_num == 0


@run_async
async def test_connection_reconnects_after_close():

    world = FakeWorld()
    channel = _channel(world)

    with patch_connect(world):
        first = await channel.get_channel()
        await channel.close_channel(first)
        await channel.try_close_connection()

        assert channel._conn is None
        assert world.connection.closed is True

        # 已关闭时重复调用不触发重连也不报错（幂等）
        await channel.try_close_connection()
        assert len(world.urls) == 1

        second = await channel.get_channel()
        assert len(world.urls) == 2
        assert channel._conn is world.connection
        assert world.connection.closed is False
        assert second.closed is False

        await channel.close_channel(second)
        await channel.try_close_connection()

    assert len(world.urls) == 2
    assert channel._active_channel_num == 0
