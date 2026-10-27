# coding: utf-8

import asyncio
import time
import typing as t

from messor.ratelimit.storage.base import BaseRateLimitStorage

_SWEEP_INTERVAL = 15.0

# 过期时刻（time.monotonic 秒），None 表示永不过期
Expire = float | None
# 内存条目：(任意值, 过期时刻)
_Entry = tuple[t.Any, Expire]


class MemoryRateLimitStorage(BaseRateLimitStorage):
    """内存实现：dict + asyncio.Lock，key 带过期时间"""

    __slots__ = (
        "_lock",
        "_data",
        "_last_sweep",
    )

    def __init__(self) -> None:

        self._lock = asyncio.Lock()
        self._data: dict[str, _Entry] = {}
        self._last_sweep = time.monotonic()

    async def incr(self, key: str, expire: float | None = None) -> int:

        async with self._lock:
            self._sweep()
            now = time.monotonic()
            entry = self._data.get(key)
            if entry is None or (entry[1] is not None and entry[1] <= now):
                count = 1
            else:
                count = entry[0] + 1
            self._data[key] = (count, now + expire if expire is not None else None)
            return count

    async def get(self, key: str) -> int | None:

        async with self._lock:
            now = time.monotonic()
            entry = self._data.get(key)
            if entry is None or (entry[1] is not None and entry[1] <= now):
                return None
            return entry[0]

    async def delete(self, key: str) -> None:

        async with self._lock:
            self._data.pop(key, None)

    async def update(
            self,
            key: str,
            fn: t.Callable[[t.Any], tuple[t.Any, t.Any]],
            *,
            expire: float | None = None) -> t.Any:

        async with self._lock:
            self._sweep()
            now = time.monotonic()
            entry = self._data.get(key)
            old = None
            if entry is not None and (entry[1] is None or entry[1] > now):
                old = entry[0]
            new, result = fn(old)
            self._data[key] = (new, now + expire if expire is not None else None)
            return result

    def _sweep(self) -> None:
        """周期性清理已过期条目，避免分片 key 无限累积"""

        now = time.monotonic()
        if now - self._last_sweep < _SWEEP_INTERVAL:
            return
        self._last_sweep = now
        expired = [
            key for key, (_, expire_at) in self._data.items()
            if expire_at is not None and expire_at <= now
        ]
        for key in expired:
            self._data.pop(key, None)
