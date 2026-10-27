# coding: utf-8

import asyncio
import time

import pytest

from messor.ratelimit import MemoryRateLimitStorage

from tests.helpers import run_async


@run_async
async def test_incr_counts_and_returns_current():

    storage = MemoryRateLimitStorage()

    assert await storage.incr("a") == 1
    assert await storage.incr("a") == 2
    assert await storage.incr("b") == 1
    assert await storage.get("a") == 2


@run_async
async def test_incr_with_expire_expires_after_ttl():

    storage = MemoryRateLimitStorage()

    assert await storage.incr("a", expire=0.05) == 1
    await asyncio.sleep(0.1)
    assert await storage.get("a") is None
    # 过期后重新计数
    assert await storage.incr("a") == 1


@run_async
async def test_get_returns_none_for_missing_key():

    storage = MemoryRateLimitStorage()

    assert await storage.get("missing") is None


@run_async
async def test_delete_removes_key():

    storage = MemoryRateLimitStorage()

    await storage.incr("a")
    await storage.delete("a")
    assert await storage.get("a") is None


@run_async
async def test_incr_without_expire_never_expires():

    storage = MemoryRateLimitStorage()

    await storage.incr("a")
    await asyncio.sleep(0.05)
    assert await storage.get("a") == 1


@run_async
async def test_sweep_removes_expired_entries():

    storage = MemoryRateLimitStorage()

    await storage.incr("expired", expire=0.01)
    await storage.incr("alive", expire=None)
    # 强制触发清理（跳过 60s 间隔）
    storage._last_sweep = time.monotonic() - 61.0
    await asyncio.sleep(0.02)
    await storage.incr("trigger")

    assert await storage.get("expired") is None
    assert await storage.get("alive") == 1
    assert await storage.get("trigger") == 1


@run_async
async def test_concurrent_incr_is_atomic():

    storage = MemoryRateLimitStorage()

    counts = await asyncio.gather(*[storage.incr("a") for _ in range(50)])

    assert counts == list(range(1, 51))
    assert await storage.get("a") == 50


@run_async
async def test_update_runs_fn_and_returns_result():

    storage = MemoryRateLimitStorage()

    result = await storage.update("a", lambda old: ((old or 0) + 1, (old or 0) + 10))

    assert result == 10
    assert await storage.get("a") == 1


@run_async
async def test_update_old_is_none_for_missing_key():

    storage = MemoryRateLimitStorage()

    received = await storage.update("missing", lambda old: (1, old))

    assert received is None


@run_async
async def test_update_writes_state_and_reexecutes_fn_on_later_reads():

    storage = MemoryRateLimitStorage()
    seen = []

    def fn(old):
        seen.append(old)
        return (1, 1)

    await storage.update("a", fn)
    await storage.update("a", fn)

    assert seen == [None, 1]


@run_async
async def test_update_with_expire_expires_after_ttl():

    storage = MemoryRateLimitStorage()

    await storage.update("a", lambda old: (1, 1), expire=0.05)
    await asyncio.sleep(0.1)
    # 过期后再次 update，old 回到 None
    received = await storage.update("a", lambda old: (1, old))

    assert received is None


@run_async
async def test_update_concurrent_is_atomic():

    storage = MemoryRateLimitStorage()
    # 50 个并发原子读改写：每个把计数 +1 并返回自己的序号
    results = await asyncio.gather(*[
        storage.update("a", lambda old: ((old or 0) + 1, (old or 0) + 1))
        for _ in range(50)
    ])

    assert sorted(results) == list(range(1, 51))
    assert await storage.get("a") == 50


@pytest.mark.parametrize("rate,limit,window_ns", [
    ("3/s", 3, 1_000_000_000),
    ("100/s", 100, 1_000_000_000),
    ("2/m", 2, 60_000_000_000),
    (" 5/S ", 5, 1_000_000_000),
    ("10/h", 10, 3_600_000_000_000),
    ("5/d", 5, 86_400_000_000_000),
])
@run_async
async def test_fixed_window_parses_rate_format(rate, limit, window_ns):

    from messor.ratelimit import FixedWindowRateLimiter

    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate=rate)

    assert limiter._limit == limit
    assert limiter._window_ns == window_ns
