# coding: utf-8

import asyncio
import time

from messor.ratelimit.base import BaseRateLimiter
from messor.ratelimit.storage.base import BaseRateLimitStorage
from messor.ratelimit.utils import (
    NANOS_PER_SECOND,
    UNIT_NS,
    build_rate_key,
    parse_rate,
    warn_long_window_memory_usage,
)


class TokenBucketRateLimiter(BaseRateLimiter):
    """令牌桶限流：以 ``100/s``、``50/m``、``10/h``、``5/d`` 指定补充速率，
    桶容量默认等于 n（可一次性突发 n 条），令牌耗尽后按速率平滑补充，无令牌则阻塞等待。

    与固定窗口的区别：令牌桶把突发平滑掉——不会出现固定窗口在边界处
    「前一秒末 + 后一秒初」连放 2n 条的双倍突发。

    桶状态（``(tokens, last_ns)``）保存在存储层，由 ``update`` 原子读改写
    （内存版锁内执行，Redis 版 CAS），算法层不持锁：不满足则睡眠后重试。
    ``_refill_take`` 是纯函数，可安全重跑。
    """

    __slots__ = (
        "_storage",
        "_key",
        "_tokens_ns_per_token",
        "_capacity",
    )

    def __init__(
            self,
            storage: BaseRateLimitStorage,
            *,
            rate: str,
            burst: int | None = None,
            key: str = "default") -> None:

        n, unit = parse_rate(rate)
        warn_long_window_memory_usage(storage, unit)
        if burst is not None and burst <= 0:
            raise ValueError("burst must be > 0")

        self._storage = storage
        self._key = build_rate_key(key)
        # 每补充 1 个令牌需要的纳秒数
        self._tokens_ns_per_token = UNIT_NS[unit] / n
        self._capacity = float(burst if burst is not None else n)

    def _refill_take(self, old: tuple[float, int] | None) -> tuple[tuple[float, int], tuple[bool, float]]:
        """纯函数：按流逝时间补充令牌并尝试取走 1 个。

        返回 ``(新状态, (granted, need_ns))``；首次使用（old 为 None）按满桶初始化。
        """

        now_ns = time.monotonic_ns()
        if old is None:
            tokens, last_ns = self._capacity, now_ns
        else:
            tokens, last_ns = old
        elapsed = now_ns - last_ns
        if elapsed > 0:
            # 按经过时间补充令牌，不超过桶容量
            tokens = min(self._capacity, tokens + elapsed / self._tokens_ns_per_token)
            last_ns = now_ns
        if tokens >= 1.0:
            return (tokens - 1.0, last_ns), (True, 0.0)
        # 还差 1 个令牌，返回需要等待的纳秒数
        need_ns = self._tokens_ns_per_token * (1.0 - tokens)
        return (tokens, last_ns), (False, need_ns)

    async def acquire(self) -> None:

        while True:
            granted, need_ns = await self._storage.update(self._key, self._refill_take)
            if granted:
                return
            # 令牌不足，睡到攒够为止再重试
            await asyncio.sleep(max(need_ns / NANOS_PER_SECOND, 0.0))
