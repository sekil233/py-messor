# coding: utf-8

import abc
import typing as t


class BaseRateLimitStorage(metaclass=abc.ABCMeta):
    """限流计数存储：只提供原子计数原语，不关心限流语义"""

    @abc.abstractmethod
    async def incr(self, key: str, expire: float | None = None) -> int:
        """原子自增并返回计数；key 不存在或已过期则从 1 开始。

        expire 秒后该 key 过期（None 表示永不过期）。
        """

        pass

    @abc.abstractmethod
    async def get(self, key: str) -> int | None:
        """读取当前计数；key 不存在或已过期返回 None"""

        pass

    @abc.abstractmethod
    async def delete(self, key: str) -> None:
        """删除 key"""

        pass

    @abc.abstractmethod
    async def update(
            self,
            key: str,
            fn: t.Callable[[t.Any], tuple[t.Any, t.Any]],
            *,
            expire: float | None = None) -> t.Any:
        """原子读改写：读取 key 当前值（不存在或已过期则为 None），
        fn 计算 ``(新值, 结果)``，写回新值并返回结果。

        内存版在锁内执行 fn；Redis 版可用 WATCH/MULTI/EXEC（CAS）实现，
        fn 在客户端执行，冲突时自动重试。fn 必须是纯函数
        （只依赖旧值与算法配置，无副作用，可安全重跑）。
        """

        pass
