# coding: utf-8

import asyncio

import pytest

from messor.serializer.raw import Serializer
from messor.backends.queue.channel import Channel

from tests.helpers import run_async


@run_async
async def test_channel_put_then_get_returns_same_item():

    channel: Channel[bytes] = Channel(Serializer, maxsize=2)

    assert channel.serializer is Serializer

    assert isinstance(channel.queue, asyncio.Queue)
    assert channel.queue.maxsize == 2

    await channel.queue.put(b"one")
    await channel.queue.put(b"two")

    assert await channel.queue.get() == b"one"
    assert await channel.queue.get() == b"two"


@run_async
async def test_channel_empty_follows_queue_contents():

    channel: Channel[int] = Channel(Serializer, maxsize=2)

    assert channel.queue.empty() is True

    await channel.queue.put(1)

    assert channel.queue.empty() is False

    await channel.queue.get()

    assert channel.queue.empty() is True


@run_async
async def test_channel_join_waits_until_task_done():

    channel: Channel[str] = Channel(Serializer, maxsize=1)
    await channel.queue.put("item")
    done = False

    async def finish() -> None:

        nonlocal done
        await channel.queue.get()
        channel.queue.task_done()
        done = True

    worker = asyncio.create_task(finish())
    await channel.queue.join()
    await worker

    assert done is True


@run_async
async def test_channel_task_done_without_get_raises():

    channel: Channel[int] = Channel(Serializer, maxsize=1)

    with pytest.raises(ValueError):
        channel.queue.task_done()
