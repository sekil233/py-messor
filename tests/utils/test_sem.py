# coding: utf-8

import asyncio

from messor.utils.sem import JoinableSemaphore

from tests.helpers import run_async


@run_async
async def test_acquire_increments_count_and_clears_event():

    sem = JoinableSemaphore(2)

    assert sem._count == 0
    assert sem._event.is_set()

    ok = await sem.acquire()

    assert ok is True
    assert sem._count == 1
    assert not sem._event.is_set()


@run_async
async def test_release_decrements_count_and_sets_event_when_idle():

    sem = JoinableSemaphore(2)
    await sem.acquire()
    await sem.acquire()

    sem.release()
    assert sem._count == 1
    assert not sem._event.is_set()

    sem.release()
    assert sem._count == 0
    assert sem._event.is_set()


@run_async
async def test_release_does_not_go_below_zero():

    sem = JoinableSemaphore(1)

    sem.release()
    assert sem._count == 0
    assert sem._event.is_set()


@run_async
async def test_join_returns_immediately_when_idle():

    sem = JoinableSemaphore(1)

    await asyncio.wait_for(sem.join(), timeout=0.5)


@run_async
async def test_join_waits_until_all_permits_are_released():

    sem = JoinableSemaphore(2)
    await sem.acquire()
    await sem.acquire()
    finished = []

    async def release_later():
        await asyncio.sleep(0.05)
        sem.release()
        finished.append("first")
        await asyncio.sleep(0.05)
        sem.release()
        finished.append("second")

    task = asyncio.create_task(release_later())
    await asyncio.wait_for(sem.join(), timeout=1)

    assert finished == ["first", "second"]
    assert sem._count == 0
    await task


@run_async
async def test_acquire_blocks_when_pool_is_full():

    sem = JoinableSemaphore(1)
    await sem.acquire()
    acquired = asyncio.Event()

    async def extra_acquire():
        await sem.acquire()
        acquired.set()

    task = asyncio.create_task(extra_acquire())
    await asyncio.sleep(0.05)

    assert not acquired.is_set()

    sem.release()
    await asyncio.wait_for(task, timeout=1)

    assert acquired.is_set()
    assert sem._count == 1
