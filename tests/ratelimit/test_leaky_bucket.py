# coding: utf-8

import asyncio
import time
from unittest.mock import patch

import pytest

from messor.ratelimit import LeakyBucketRateLimiter, MemoryRateLimitStorage

from tests.helpers import run_async
from tests.ratelimit.conftest import FakeClock, wire_clock


@pytest.mark.parametrize("rate", ["0/s", "-1/s", "100/x", "1.5/s", "abc", "100", ""])
def test_leaky_bucket_rejects_invalid_rate(rate):

    with pytest.raises(ValueError):
        LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate=rate)


@pytest.mark.parametrize("rate,interval_ns", [
    ("5/s", 0.2),
    ("2/m", 30.0),
    ("10/h", 360.0),
    ("5/d", 17280.0),
])
@run_async
async def test_leaky_bucket_parses_rate(rate, interval_ns):

    limiter = LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate=rate)

    assert limiter._interval_ns == pytest.approx(interval_ns * 1e9)


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_leaky_bucket_first_passes_immediately(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await limiter.acquire()

    assert clock.now == 0.0  # 首个请求立即放行


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_leaky_bucket_no_burst_strictly_spaced(mock_monotonic_ns, mock_monotonic, mock_sleep):

    # 漏桶无突发：5 个并发按 0.2s 间隔逐一放行，总耗时 0.8s
    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await asyncio.gather(*[limiter.acquire() for _ in range(5)])

    assert clock.now == 0.8


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_leaky_bucket_contrast_with_token_bucket(mock_monotonic_ns, mock_monotonic, mock_sleep):

    # 同参数下：令牌桶 5 条瞬时通过，漏桶第 2 条就要等 0.2s
    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await limiter.acquire()
    await limiter.acquire()

    assert clock.now == 0.2


@run_async
async def test_leaky_bucket_real_time_smoke():

    # 真实时间冒烟：5/s 下第 2 条需等 0.2s 间隔（漏桶无突发）
    limiter = LeakyBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")
    await limiter.acquire()

    t0 = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - t0

    assert 0.15 <= elapsed <= 0.4
