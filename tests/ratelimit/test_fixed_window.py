# coding: utf-8

import asyncio
import time
from unittest.mock import patch

import pytest

from messor.ratelimit import FixedWindowRateLimiter, MemoryRateLimitStorage

from tests.helpers import run_async
from tests.ratelimit.conftest import FakeClock, wire_clock


@pytest.mark.parametrize("rate", ["0/s", "-1/s", "100/x", "1.5/s", "abc", "100", ""])
def test_fixed_window_rejects_invalid_rate(rate):

    with pytest.raises(ValueError):
        FixedWindowRateLimiter(MemoryRateLimitStorage(), rate=rate)


@run_async
async def test_fixed_window_passes_within_limit():

    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="3/s")

    for _ in range(3):
        await limiter.acquire()


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_fixed_window_blocks_until_next_window(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="3/s")

    for _ in range(3):
        await limiter.acquire()
    assert clock.now == 0.0  # 窗口内 3 条瞬时放行

    await limiter.acquire()  # 第 4 条阻塞到下一窗口

    assert clock.now == 1.0  # 时钟精确推进到窗口边界


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_fixed_window_new_window_passes_immediately(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="2/s")

    await limiter.acquire()  # 窗口 N 名额 1
    await limiter.acquire()  # 窗口 N 名额 2
    await limiter.acquire()  # 阻塞到 N+1 窗口（占其名额 1）
    assert clock.now == 1.0

    now_before = clock.now
    await limiter.acquire()  # N+1 窗口剩余名额，立即放行

    assert clock.now == now_before  # 时钟未推进


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_fixed_window_burst_spans_multiple_windows(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="2/s")

    await asyncio.gather(*[limiter.acquire() for _ in range(10)])

    # 2/s 下 10 个并发严格跨 5 个窗口
    assert clock.now == 4.0


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_fixed_window_strictly_limits_each_window(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="3/s")

    # 9 个并发：每窗口恰好 3 条，共跨 3 个窗口
    await asyncio.gather(*[limiter.acquire() for _ in range(9)])

    assert clock.now == 2.0


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_fixed_window_strictly_limits_across_times_in_window(mock_monotonic_ns, mock_monotonic, mock_sleep):

    # 同一窗口内不同时刻到达：计数必须合并到同一分片 key（epoch），窗口内仍严格限流
    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="2/s")

    await limiter.acquire()  # t=0.0  窗口 N 名额 1
    clock.advance(0.5)       # t=0.5  同一窗口内
    await limiter.acquire()  #         窗口 N 名额 2
    await limiter.acquire()  #         第 3 条阻塞到窗口边界

    assert clock.now == 1.0

    await limiter.acquire()  # 新窗口立即放行

    assert clock.now == 1.0


@run_async
async def test_fixed_window_real_time_smoke():

    # 真实时间冒烟：防止假时钟与现实脱节（第 3 条阻塞 0 < dt <= 1s）
    limiter = FixedWindowRateLimiter(MemoryRateLimitStorage(), rate="2/s")
    await limiter.acquire()
    await limiter.acquire()

    t0 = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - t0

    assert 0.0 < elapsed <= 1.02
