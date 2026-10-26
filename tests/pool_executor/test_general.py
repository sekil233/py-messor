# coding: utf-8

import asyncio

import pytest

from messor.pool_executor.general import PoolExecutor
from messor.pool_executor.base import BasePoolExecutor

from tests.helpers import run_async


def test_pool_executor_is_base_subclass():

    assert issubclass(PoolExecutor, BasePoolExecutor)


@run_async
async def test_pool_executor_submit_runs_coroutine_with_args_and_kwargs():

    executor = PoolExecutor(1)
    received = {}

    async def worker(a, b, *, c):
        received["a"] = a
        received["b"] = b
        received["c"] = c

    await executor.submit(worker, (1, 2), {"c": 3})
    await executor.join()

    assert received == {"a": 1, "b": 2, "c": 3}


@pytest.mark.parametrize("maxsize", [1, 2, 3])
@run_async
async def test_pool_executor_submit_respects_maxsize(maxsize):

    executor = PoolExecutor(maxsize)
    running = 0
    max_running = 0

    async def worker():
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0.05)
        running -= 1

    for _ in range(maxsize * 3):
        await executor.submit(worker, (), {})

    await executor.join()

    assert max_running == maxsize
    assert running == 0
    assert executor._sem._count == 0


@run_async
async def test_pool_executor_join_waits_for_in_flight_tasks():

    executor = PoolExecutor(2)
    completed = []

    async def worker(idx):
        await asyncio.sleep(0.05)
        completed.append(idx)

    for idx in range(4):
        await executor.submit(worker, (idx,), {})

    await asyncio.wait_for(executor.join(), timeout=1)

    assert sorted(completed) == [0, 1, 2, 3]


@run_async
async def test_pool_executor_join_returns_immediately_when_no_tasks():

    executor = PoolExecutor(2)

    await asyncio.wait_for(executor.join(), timeout=0.5)


@run_async
async def test_pool_executor_failed_task_still_releases_slot():

    executor = PoolExecutor(1)
    ran = []

    async def boom():
        raise ValueError("boom")

    async def ok():
        ran.append("ok")

    task = await executor.submit(boom, (), {})
    with pytest.raises(ValueError, match="boom"):
        await task

    await executor.submit(ok, (), {})
    await asyncio.wait_for(executor.join(), timeout=1)

    assert ran == ["ok"]
    assert executor._sem._count == 0
