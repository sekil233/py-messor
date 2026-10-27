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


class FixedWindowRateLimiter(BaseRateLimiter):
    """固定窗口计数限流：以 ``100/s``、``50/m``、``10/h``、``5/d`` 形式指定速率
    （n 个/秒、分钟、小时、天），每个窗口内最多放行 n 条，超出阻塞到下一窗口。

    计数按窗口分片：key 形如 ``<key>:<epoch>``，epoch = now // window。
    分片 key 天然自带固定窗口语义，且不依赖进程内状态，未来换 Redis 后端同一套代码可用。

    epoch 与边界用整数纳秒（``time.monotonic_ns``）计算，避免大时间戳下
    浮点除法 ``now // 0.2`` 精度不足导致窗口边界无法翻页、限流失效。

    ``acquire()`` 不持锁：计数与放行的原子性由存储层保证（``incr`` 原子自增），
    超限者睡眠到窗口边界后重试。同一时刻醒来的等待者按原子竞争放行，
    不保证严格 FIFO，但每个窗口放行数严格不超过 limit。
    """

    __slots__ = (
        "_storage",
        "_key",
        "_limit",
        "_window_ns",
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
        self._limit = n
        self._window_ns = UNIT_NS[unit]

    async def acquire(self) -> None:

        while True:
            now_ns = time.monotonic_ns()
            epoch = now_ns // self._window_ns
            # 当前窗口剩余时间，同时作为分片 key 的过期时间
            remain_sec = ((epoch + 1) * self._window_ns - now_ns) / NANOS_PER_SECOND
            key = f"{self._key}:{epoch}"
            count = await self._storage.incr(key, expire=remain_sec)
            if count <= self._limit:
                return
            # 本窗口名额已满，睡到窗口结束再试
            await asyncio.sleep(max(remain_sec, 0.0))
