# coding: utf-8

import re
import typing as t
import warnings

from messor.ratelimit.storage.memory import MemoryRateLimitStorage

# 速率格式：``<n>/<unit>``，如 ``100/s``（100 个/秒）、``50/m``（50 个/分钟）、
# ``10/h``（10 个/小时）、``5/d``（5 个/天）
NANOS_PER_SECOND = 1_000_000_000
# 限流 key 固定前缀：与共享存储中其他用途的 key 隔离
RATE_KEY_PREFIX = "messor:rate:"
_RATE_RE = re.compile(r"^(\d+)/([smhd])$")
UNIT_NS = {
    "s": 1 * NANOS_PER_SECOND,
    "m": 60 * NANOS_PER_SECOND,
    "h": 3600 * NANOS_PER_SECOND,
    "d": 86400 * NANOS_PER_SECOND,
}


def parse_rate(rate: str) -> tuple[int, str]:
    """解析 ``100/s``、``50/m``、``10/h``、``5/d`` 为 (n, 单位)；格式非法抛 ValueError"""

    m = _RATE_RE.match(rate.strip().lower())
    if m is None:
        raise ValueError(f"invalid rate {rate!r}, expect like '100/s', '50/m', '10/h' or '5/d'")
    n = int(m.group(1))
    if n <= 0:
        raise ValueError("rate must be > 0, expect like '100/s', '50/m', '10/h' or '5/d'")
    return n, m.group(2)


def build_rate_key(key: str) -> str:
    """构造限流桶存储 key：``<RATE_KEY_PREFIX><key>``"""

    return f"{RATE_KEY_PREFIX}{key}"


def warn_long_window_memory_usage(storage: t.Any, unit: str) -> None:
    """h/d 长窗口配合内存存储时警告内存占用问题并建议改用持久化存储"""

    if unit in ("h", "d") and isinstance(storage, MemoryRateLimitStorage):
        warnings.warn(
            f"rate unit {unit!r} with MemoryRateLimitStorage keeps window/shard state "
            "in memory for the whole window and can grow memory usage; consider a "
            "persistent storage for long windows",
            stacklevel=3,
        )
