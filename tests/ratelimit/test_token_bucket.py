# coding: utf-8

import asyncio
import time
from unittest.mock import patch

import pytest

from messor.ratelimit import MemoryRateLimitStorage, TokenBucketRateLimiter

from tests.helpers import run_async
from tests.ratelimit.conftest import FakeClock, wire_clock


@pytest.mark.parametrize("rate", ["0/s", "-1/s", "100/x", "1.5/s", "abc", "100", ""])
def test_token_bucket_rejects_invalid_rate(rate):

    with pytest.raises(ValueError):
        TokenBucketRateLimiter(MemoryRateLimitStorage(), rate=rate)


def test_token_bucket_rejects_bad_burst():

    with pytest.raises(ValueError):
        TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s", burst=0)
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s", burst=-1)


@run_async
async def test_token_bucket_default_capacity_equals_rate_numerator():

    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    assert limiter._capacity == 5.0


@pytest.mark.parametrize("rate,ns_per_token", [
    ("5/s", 0.2),
    ("2/m", 30.0),
    ("10/h", 360.0),
    ("5/d", 17280.0),
])
@run_async
async def test_token_bucket_parses_rate(rate, ns_per_token):

    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate=rate)

    assert limiter._tokens_ns_per_token == pytest.approx(ns_per_token * 1e9)


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_token_bucket_allows_burst_up_to_capacity(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await asyncio.gather(*[limiter.acquire() for _ in range(5)])

    assert clock.now == 0.0  # 满桶突发不消耗时间


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_token_bucket_drains_smoothly_after_burst(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await asyncio.gather(*[limiter.acquire() for _ in range(5)])

    for _ in range(5):
        await limiter.acquire()

    # 桶空后每 0.2s 补 1 个：5 条 = 1.0s
    assert clock.now == 1.0


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_token_bucket_smooths_concurrent_burst(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")

    await asyncio.gather(*[limiter.acquire() for _ in range(10)])

    # 突发 5 + 后 5 条 ×0.2s = 1.0s（固定窗口同参数需 2s）
    assert clock.now == 1.0


@patch.object(asyncio, "sleep")
@patch.object(time, "monotonic")
@patch.object(time, "monotonic_ns")
@run_async
async def test_token_bucket_custom_burst_caps_instant_burst(mock_monotonic_ns, mock_monotonic, mock_sleep):

    clock = FakeClock()
    wire_clock(clock, mock_sleep, mock_monotonic, mock_monotonic_ns)
    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s", burst=2)
    passed_instant = 0

    async def one() -> None:
        nonlocal passed_instant
        now_before = clock.now
        await limiter.acquire()
        if clock.now == now_before:
            passed_instant += 1

    await asyncio.gather(*[one() for _ in range(10)])

    # 只有满桶的 2 条瞬时通过；其余 8 条各需 0.2s（5/s）
    assert passed_instant == 2
    assert clock.now == pytest.approx(1.6)


@run_async
async def test_token_bucket_real_time_smoke():

    # 真实时间冒烟：突发后第 6 条需等补 1 个令牌（≥0.18s）
    limiter = TokenBucketRateLimiter(MemoryRateLimitStorage(), rate="5/s")
    await asyncio.gather(*[limiter.acquire() for _ in range(5)])

    t0 = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - t0

    assert 0.15 <= elapsed <= 0.4
