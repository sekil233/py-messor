# coding: utf-8

from queue_func.serializer.orjson_ser import Serializer
from queue_func.backends.rabbit_aio_pika.resource import Resource

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect


def _resource(world: FakeWorld, **kwargs) -> Resource:

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
    return Resource(**values)


@run_async
async def test_declare_queue_is_passive_when_declare_disabled():

    world = FakeWorld()
    resource = _resource(world)

    with patch_connect(world):
        channel = await resource.get_channel()
        queue = await resource.declare_queue(channel)

    assert resource.queue_name == "jobs"
    assert queue is world.queue
    assert channel.declared == [("jobs", {"passive": True})]
    assert world.urls == ["amqp://guest:guest@127.0.0.1/"]


@run_async
async def test_declare_queue_forwards_declare_flags():

    world = FakeWorld()
    resource = _resource(
        world,
        enable_declare_queue=True,
        queue_durable=True,
        queue_exclusive=True,
        queue_passive=True,
        queue_auto_delete=True,
        queue_arguments={"x-message-ttl": 1000},
    )

    with patch_connect(world):
        channel = await resource.get_channel()
        await resource.declare_queue(channel)

    assert channel.declared == [(
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
    resource = _resource(world)

    with patch_connect(world):
        first = await resource.get_channel()
        second = await resource.get_channel()

        assert len(world.urls) == 1
        assert resource._active_channel_num == 2

        await resource.close_channel(first)
        await resource.try_close_connection()
        assert world.connection.closed is False

        await resource.close_channel(second)
        await resource.try_close_connection()

    assert first.closed is True
    assert second.closed is True
    assert world.connection.closed is True
    assert resource._active_channel_num == 0
