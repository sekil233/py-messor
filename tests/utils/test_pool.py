# coding: utf-8

import asyncio
import typing as t

import pytest

from messor.utils.pool import BaseObjectPool, ObjectPool, StaticPool

from tests.helpers import run_async


class Item:

    def __init__(self):
        self.closed = False


def _factory(items: list[Item]) -> t.Callable[[], t.Awaitable[Item]]:

    async def factory() -> Item:
        item = Item()
        items.append(item)
        return item

    return factory


async def _closer(item: Item) -> None:

    item.closed = True


@run_async
async def test_acquire_creates_lazily_and_reuses_idle():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 2, closer=_closer)

    assert items == []
    assert pool.size == 0

    first = await pool.acquire()
    assert len(items) == 1
    assert pool.size == 1

    await pool.release(first)
    second = await pool.acquire()

    assert second is first
    assert len(items) == 1
    await pool.close()


@run_async
async def test_acquire_blocks_at_maxsize_and_wakes_on_release():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer)

    first = await pool.acquire()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pool.acquire(), timeout=0.05)

    await pool.release(first)
    second = await asyncio.wait_for(pool.acquire(), timeout=0.5)

    assert second is first
    await pool.close()


@run_async
async def test_unhealthy_idle_object_is_discarded_and_recreated():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer, healthy=lambda item: not item.closed)

    first = await pool.acquire()
    await pool.release(first)
    first.closed = True

    second = await pool.acquire()

    assert second is not first
    assert first.closed is True
    assert len(items) == 2
    assert pool.size == 1
    await pool.close()


@run_async
async def test_close_drains_idle_and_rejects_further_acquire():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 2, closer=_closer)

    first = await pool.acquire()
    second = await pool.acquire()
    await pool.release(first)
    await pool.release(second)

    await pool.close()

    assert pool.closed is True
    assert pool.size == 0
    assert all(item.closed is True for item in items)
    with pytest.raises(RuntimeError):
        await pool.acquire()


@run_async
async def test_inflight_object_is_closed_on_release_after_close():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer)

    first = await pool.acquire()
    await pool.close()

    assert first.closed is False
    await pool.release(first)

    assert first.closed is True
    assert pool.size == 0


@run_async
async def test_factory_failure_does_not_leak_slot():

    calls = []

    async def factory() -> Item:
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("boom")
        return Item()

    pool = ObjectPool(factory, 1, closer=_closer)

    with pytest.raises(ValueError):
        await pool.acquire()

    assert pool.size == 0
    item = await asyncio.wait_for(pool.acquire(), timeout=0.5)

    assert isinstance(item, Item)
    assert len(calls) == 2
    await pool.close()


@run_async
async def test_hold_releases_on_success_and_discards_on_error():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer)

    async with pool.hold() as first:
        pass

    assert first.closed is False
    reused = await pool.acquire()
    assert reused is first
    await pool.release(reused)

    with pytest.raises(RuntimeError):
        async with pool.hold() as broken:
            raise RuntimeError("boom")

    assert broken.closed is True
    assert pool.size == 0
    await pool.close()


@run_async
async def test_maxsize_limits_creation_under_concurrency():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 3, closer=_closer)

    acquired = [await pool.acquire() for _ in range(3)]

    assert len(items) == 3
    assert pool.size == 3
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pool.acquire(), timeout=0.05)

    for item in acquired:
        await pool.release(item)
    await pool.close()


def test_base_object_pool_cannot_be_instantiated():

    with pytest.raises(TypeError):
        BaseObjectPool(_factory([]))


def test_invalid_maxsize_is_rejected():

    with pytest.raises(ValueError):
        ObjectPool(_factory([]), 0)


@run_async
async def test_concurrent_acquire_never_exceeds_maxsize():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 3, closer=_closer)
    gate = asyncio.Event()
    counter_lock = asyncio.Lock()
    current = 0
    peak = 0

    async def worker() -> None:
        nonlocal current, peak
        obj = await pool.acquire()
        async with counter_lock:
            current += 1
            peak = max(peak, current)
        await gate.wait()
        async with counter_lock:
            current -= 1
        await pool.release(obj)

    tasks = [asyncio.create_task(worker()) for _ in range(10)]
    for _ in range(100):
        if peak >= 3:
            break
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)

    assert peak == 3
    assert len(items) == 3
    assert pool.size == 3

    gate.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)

    # 排队者复用了归还的对象，没有额外创建
    assert len(items) == 3
    await pool.close()


@run_async
async def test_factory_runs_outside_lock_so_creation_is_parallel():

    async def factory() -> Item:
        await asyncio.sleep(0.1)
        return Item()

    pool = ObjectPool(factory, 4, closer=_closer)
    start = asyncio.get_running_loop().time()
    items = await asyncio.wait_for(
        asyncio.gather(*(pool.acquire() for _ in range(4))), timeout=2)
    elapsed = asyncio.get_running_loop().time() - start

    assert len(items) == 4
    # 持锁创建会串行累加约 0.4s
    assert elapsed < 0.3
    await pool.close()


@run_async
async def test_close_wakes_all_waiting_acquires():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer)

    held = await pool.acquire()
    waiters = [asyncio.create_task(pool.acquire()) for _ in range(3)]
    await asyncio.sleep(0.05)

    assert not any(task.done() for task in waiters)

    await pool.close()

    for task in waiters:
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(task, timeout=1)

    # 在途对象不受 close 影响，归还时才关闭
    assert held.closed is False
    await pool.release(held)
    assert held.closed is True


@run_async
async def test_release_unhealthy_object_frees_slot_for_waiter():

    items: list[Item] = []
    pool = ObjectPool(
        _factory(items), 1, closer=_closer, healthy=lambda item: not item.closed)

    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.05)

    assert not waiter.done()

    # 归还时不健康：直接关闭并腾出名额，唤醒等待者去新建
    held.closed = True
    await pool.release(held)

    got = await asyncio.wait_for(waiter, timeout=1)

    assert got is not held
    assert len(items) == 2
    assert pool.size == 1
    await pool.close()


@run_async
async def test_discard_frees_slot_for_waiter():

    items: list[Item] = []
    pool = ObjectPool(_factory(items), 1, closer=_closer)

    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0.05)

    await pool.discard(held)
    got = await asyncio.wait_for(waiter, timeout=1)

    assert got is not held
    assert held.closed is True
    assert len(items) == 2
    await pool.close()


@run_async
async def test_eviction_keeps_remaining_idle_items_reusable():

    items: list[Item] = []
    pool = ObjectPool(
        _factory(items), 2, closer=_closer, healthy=lambda item: not item.closed)

    first = await pool.acquire()
    second = await pool.acquire()
    await pool.release(first)
    await pool.release(second)
    # 栈顶的 second 失效，acquire 会先淘汰它、再复用下面的 first
    second.closed = True

    got = await pool.acquire()

    assert got is first
    assert second.closed is True
    assert len(items) == 2
    assert pool.size == 1
    await pool.release(got)
    await pool.close()


@run_async
async def test_static_pool_acquire_creates_lazily_and_shares_single_slot():

    items: list[Item] = []
    holder = StaticPool(_factory(items), closer=_closer)

    assert items == []

    first = await holder.acquire()
    second = await holder.acquire()

    assert second is first
    assert len(items) == 1

    # release 无操作：槽常驻，之后仍复用同一对象
    await holder.release(first)
    assert await holder.acquire() is first
    assert len(items) == 1
    await holder.close()


@run_async
async def test_static_pool_concurrent_acquire_creates_only_once():

    created = 0
    gate = asyncio.Event()

    async def factory() -> Item:
        nonlocal created
        created += 1
        await gate.wait()
        return Item()

    holder = StaticPool(factory, closer=_closer)
    acquires = [asyncio.create_task(holder.acquire()) for _ in range(5)]
    await asyncio.sleep(0.05)
    gate.set()
    slots = await asyncio.gather(*acquires)

    assert created == 1
    assert all(slot is slots[0] for slot in slots)
    await holder.close()


@run_async
async def test_static_pool_rebuilds_unhealthy_slot_on_acquire():

    items: list[Item] = []
    holder = StaticPool(
        _factory(items), closer=_closer, healthy=lambda item: not item.closed)

    first = await holder.acquire()
    first.closed = True

    second = await holder.acquire()

    assert second is not first
    assert len(items) == 2
    await holder.close()


@run_async
async def test_static_pool_discard_rebuilds_next_acquire():

    items: list[Item] = []
    holder = StaticPool(_factory(items), closer=_closer)

    first = await holder.acquire()
    await holder.discard(first)

    assert first.closed is True

    second = await holder.acquire()

    assert second is not first
    assert len(items) == 2
    await holder.close()


@run_async
async def test_static_pool_discard_ignores_stale_object():

    items: list[Item] = []
    holder = StaticPool(_factory(items), closer=_closer)

    first = await holder.acquire()
    await holder.discard(first)
    second = await holder.acquire()

    # first 已非当前槽，重复 discard 不应误关已重建的 second
    await holder.discard(first)

    assert second.closed is False
    assert len(items) == 2
    await holder.close()


@run_async
async def test_static_pool_close_is_idempotent_and_rejects_acquire():

    items: list[Item] = []
    holder = StaticPool(_factory(items), closer=_closer)

    slot = await holder.acquire()
    await holder.close()
    await holder.close()

    assert holder.closed is True
    assert slot.closed is True
    assert len(items) == 1
    with pytest.raises(RuntimeError):
        await holder.acquire()


@run_async
async def test_static_pool_hold_reuses_on_success_and_discards_on_error():

    items: list[Item] = []
    holder = StaticPool(_factory(items), closer=_closer)

    async with holder.hold() as first:
        pass

    assert first.closed is False
    assert await holder.acquire() is first

    with pytest.raises(RuntimeError):
        async with holder.hold() as broken:
            raise RuntimeError("boom")

    # 复用的还是同一个槽，出错即被丢弃关闭；重建发生在下次 acquire
    assert broken is first
    assert broken.closed is True
    assert len(items) == 1

    rebuilt = await holder.acquire()
    assert rebuilt is not broken
    assert len(items) == 2
    await holder.close()

