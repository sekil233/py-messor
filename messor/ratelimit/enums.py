# coding: utf-8

import importlib
from enum import StrEnum, unique

from messor.ratelimit.base import BaseRateLimiter
from messor.ratelimit.fixed_window import FixedWindowRateLimiter
from messor.ratelimit.leaky_bucket import LeakyBucketRateLimiter
from messor.ratelimit.token_bucket import TokenBucketRateLimiter
from messor.ratelimit.storage.base import BaseRateLimitStorage


@unique
class RateLimiter(StrEnum):
    """限流算法枚举"""

    TOKEN_BUCKET = "token_bucket"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@unique
class RateLimitStorage(StrEnum):
    """限流计数存储枚举"""

    MEMORY = "memory"


# 算法都在本包内、无可选依赖，直接映射
_RATE_LIMITER_CLS: dict[str, type[BaseRateLimiter]] = {
    RateLimiter.TOKEN_BUCKET: TokenBucketRateLimiter,
    RateLimiter.FIXED_WINDOW: FixedWindowRateLimiter,
    RateLimiter.LEAKY_BUCKET: LeakyBucketRateLimiter,
}

# 存储可能依赖可选第三方库（如未来 redis 客户端），惰性导入，
# 缺失时不影响内存等其余实现
_RATE_LIMIT_STORAGE_LOC: dict[str, tuple[str, str]] = {
    RateLimitStorage.MEMORY: ("messor.ratelimit.storage.memory", "MemoryRateLimitStorage"),
}


def _load_storage_cls(storage: RateLimitStorage) -> type[BaseRateLimitStorage]:
    """惰性导入存储实现类；可选依赖缺失时报出清晰错误"""

    module_path, cls_name = _RATE_LIMIT_STORAGE_LOC[storage]
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(
            f"加载限流存储实现失败（模块 {module_path!r} 不可用，"
            "可能是缺少其可选依赖）：{e}") from e
    return getattr(module, cls_name)


def build_rate_limiter(
        rate_limiter: RateLimiter,
        rate_limit_storage: RateLimitStorage,
        *,
        rate: str,
        key: str) -> BaseRateLimiter:
    """按枚举构建限流器：算法直接映射，存储惰性导入"""

    return _RATE_LIMITER_CLS[rate_limiter](
        _load_storage_cls(rate_limit_storage)(),
        rate=rate,
        key=key,
    )
