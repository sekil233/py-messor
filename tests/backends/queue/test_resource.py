# coding: utf-8

import asyncio

import pytest

from queue_func.serializer.raw import Serializer
from queue_func.backends.queue.resource import Resource

from tests.helpers import run_async


@run_async
async def test_resource_put_then_get_returns_same_item():

    resource: Resource[bytes] = Resource(Serializer, maxsize=2)

    assert resource.serializer is Serializer

    assert isinstance(resource.queue, asyncio.Queue)
    assert resource.queue.maxsize == 2

    await resource.queue.put(b"one")
    await resource.queue.put(b"two")

    assert await resource.queue.get() == b"one"
    assert await resource.queue.get() == b"two"


@run_async
async def test_resource_empty_follows_queue_contents():

    resource: Resource[int] = Resource(Serializer, maxsize=2)

    assert resource.queue.empty() is True

    await resource.queue.put(1)

    assert resource.queue.empty() is False

    await resource.queue.get()

    assert resource.queue.empty() is True


@run_async
async def test_resource_join_waits_until_task_done():

    resource: Resource[str] = Resource(Serializer, maxsize=1)
    await resource.queue.put("item")
    done = False

    async def finish() -> None:

        nonlocal done
        await resource.queue.get()
        resource.queue.task_done()
        done = True

    worker = asyncio.create_task(finish())
    await resource.queue.join()
    await worker

    assert done is True


@run_async
async def test_resource_task_done_without_get_raises():

    resource: Resource[int] = Resource(Serializer, maxsize=1)

    with pytest.raises(ValueError):
        resource.queue.task_done()
