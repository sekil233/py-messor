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


class LeakyBucketRateLimiter(BaseRateLimiter):
    """漏桶限流：以 ``100/s``、``50/m``、``10/h``、``5/d`` 指定固定输出速率，
    每个请求分配到时间轴上等间隔的槽位，严格恒定速率放行，突发被完全平滑——没有瞬时突发。

    与令牌桶的区别：令牌桶允许突发（桶容量内瞬时通过）；漏桶无论多少请求
    并发到达，都以固定速率逐一放行。

    槽位时刻（``next_leak_ns``）保存在存储层，由 ``update`` 原子读改写
    （内存版锁内执行，Redis 版 CAS），算法层不持锁。``_alloc_slot`` 是纯函数：
    只有实际放行（槽位已到）才推进 next，等待期间不预占槽位，可安全重跑。
    """

    __slots__ = (
        "_storage",
        "_key",
        "_interval_ns",
    )

    def __init__(
            self,
            storage: BaseRateLimitStorage,
            *,
            rate: str,
            key: str = "default") -> None:

        n, unit = parse_rate(rate)
        warn_long_window_memory_usage(storage, unit)

        self._storage = storage
        self._key = build_rate_key(key)
        # 放行间隔：每 1 条所需的纳秒数
        self._interval_ns = UNIT_NS[unit] / n

    def _alloc_slot(self, old: int | None) -> tuple[int, tuple[float, bool]]:
        """纯函数：尝试占用下一个放行槽位。

        返回 ``(新状态, (wait_sec, granted))``；只有槽位已到（wait<=0）才推进
        ``next`` 到下一槽位，等待期间 next 不变（避免重试时预占导致持续推远）。
        """

        now_ns = time.monotonic_ns()
        next_leak_ns = now_ns if old is None else old
        slot_ns = max(now_ns, next_leak_ns)
        wait_ns = slot_ns - now_ns
        if wait_ns > 0:
            # 槽位未到：next 保持原样，等待后重试
            return next_leak_ns, (wait_ns / NANOS_PER_SECOND, False)
        # 槽位已到：占用本槽位并推进到下一槽位
        return next_leak_ns + self._interval_ns, (0.0, True)

    async def acquire(self) -> None:

        while True:
            wait_sec, granted = await self._storage.update(self._key, self._alloc_slot)
            if granted:
                return
            # 槽位未到，睡到点再试
            await asyncio.sleep(max(wait_sec, 0.0))
