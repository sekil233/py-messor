# coding: utf-8

import abc
import asyncio
import typing as t
from collections import deque
from contextlib import asynccontextmanager

_T = t.TypeVar("_T")


class BaseObjectPool(t.Generic[_T], abc.ABC):
    """对象池接口：``acquire``/``release``/``discard``/``close`` 由子类实现

    ``hold`` 为共用封装：借出并在退出时归还，块内抛异常则丢弃。
    """

    __slots__ = (
        "_factory",
        "_closer",
        "_healthy",
        "_closed",
    )

    def __init__(
            self,
            factory: t.Callable[[], t.Awaitable[_T]],
            *,
            closer: t.Callable[[_T], t.Awaitable[None]] | None = None,
            healthy: t.Callable[[_T], bool] | None = None):

        self._factory = factory
        self._closer = closer
        self._healthy = healthy
        self._closed = False

    @property
    def closed(self) -> bool:

        return self._closed

    @abc.abstractmethod
    async def acquire(self) -> _T:
        """借出对象"""

        pass

    @abc.abstractmethod
    async def release(self, obj: _T) -> None:
        """归还对象；不池化实现可为无操作"""

        pass

    @abc.abstractmethod
    async def discard(self, obj: _T) -> None:
        """丢弃对象并关闭"""

        pass

    @abc.abstractmethod
    async def close(self) -> None:
        """关闭池：不再分配，回收已创建的对象"""

        pass

    @asynccontextmanager
    async def hold(self) -> t.AsyncGenerator[_T, None]:
        """借出并在退出时归还；块内抛异常视为对象状态未知，直接丢弃"""

        obj = await self.acquire()
        try:
            yield obj
        except BaseException:
            await self.discard(obj)
            raise
        else:
            await self.release(obj)

    def _is_healthy(self, obj: _T) -> bool:

        if self._healthy is None:
            return True
        return self._healthy(obj)

    async def _close_obj(self, obj: _T) -> None:

        if self._closer is None:
            return None
        try:
            await self._closer(obj)
        except Exception:
            # 单个对象关闭失败不应中断其余对象的回收
            pass


class ObjectPool(BaseObjectPool[_T]):
    """通用对象池：懒创建、上限控制、健康检查与统一关闭

    - 对象由 ``factory`` 按需创建，总数不超过 ``maxsize``
    - ``healthy`` 在借出与归还时校验，不健康的对象直接丢弃并回收名额
    - 关闭后不再分配：空闲对象立即关闭，在途对象在归还时关闭
    """

    __slots__ = (
        "_maxsize",
        "_cond",
        "_idle",
        "_total",
    )

    def __init__(
            self,
            factory: t.Callable[[], t.Awaitable[_T]],
            maxsize: int,
            *,
            closer: t.Callable[[_T], t.Awaitable[None]] | None = None,
            healthy: t.Callable[[_T], bool] | None = None):

        if maxsize < 1:
            raise ValueError(f"maxsize must be >= 1, got {maxsize}")

        super().__init__(factory, closer=closer, healthy=healthy)
        self._maxsize = maxsize
        self._cond = asyncio.Condition()
        self._idle: deque[_T] = deque()
        self._total = 0

    @property
    def size(self) -> int:
        """已创建的对象总数，含借出中的"""

        return self._total

    async def acquire(self) -> _T:
        """借出对象：优先复用空闲对象，未达上限则新建，否则等待"""

        to_close: list[_T] = []
        reserved = False
        try:
            async with self._cond:
                while True:
                    if self._closed:
                        raise RuntimeError("acquire on closed pool")
                    while self._idle:
                        obj = self._idle.pop()
                        if self._is_healthy(obj):
                            return obj
                        self._total -= 1
                        to_close.append(obj)
                        self._cond.notify()
                    if self._total < self._maxsize:
                        self._total += 1
                        reserved = True
                        break
                    await self._cond.wait()
            return await self._factory()
        except BaseException:
            if reserved:
                async with self._cond:
                    self._total -= 1
                    self._cond.notify()
            raise
        finally:
            for obj in to_close:
                await self._close_obj(obj)

    async def release(self, obj: _T) -> None:
        """归还对象；池已关闭或对象不健康时直接关闭"""

        async with self._cond:
            if not self._closed and self._is_healthy(obj):
                self._idle.append(obj)
                self._cond.notify()
                return
            self._total -= 1
            self._cond.notify()
        await self._close_obj(obj)

    async def discard(self, obj: _T) -> None:
        """丢弃对象：回收名额并关闭"""

        async with self._cond:
            self._total -= 1
            self._cond.notify()
        await self._close_obj(obj)

    async def close(self) -> None:
        """关闭池：关闭全部空闲对象，等待借出中的对象归还后关闭"""

        async with self._cond:
            if self._closed:
                return None
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._total -= len(idle)
            self._cond.notify_all()
        for obj in idle:
            await self._close_obj(obj)
        return None


class StaticPool(BaseObjectPool[_T]):
    """静态单槽：不池化场景下与 ``ObjectPool`` 同接口的实现（同 SQLAlchemy ``StaticPool`` 语义）

    懒建一个对象并全程共享：``release`` 无操作，使用方并发拿到同一对象；
    对象不健康或被 ``discard`` 后在下次 ``acquire`` 时重建。
    """

    __slots__ = (
        "_lock",
        "_obj",
    )

    def __init__(
            self,
            factory: t.Callable[[], t.Awaitable[_T]],
            *,
            closer: t.Callable[[_T], t.Awaitable[None]] | None = None,
            healthy: t.Callable[[_T], bool] | None = None):

        super().__init__(factory, closer=closer, healthy=healthy)
        self._lock = asyncio.Lock()
        self._obj: _T | None = None

    async def acquire(self) -> _T:
        """取对象：健康则直接复用；不存在或不健康时加锁重建"""

        if self._closed:
            raise RuntimeError("acquire on closed pool")
        obj = self._obj
        if obj is not None and self._is_healthy(obj):
            return obj
        async with self._lock:
            if self._closed:
                raise RuntimeError("acquire on closed pool")
            obj = self._obj
            if obj is not None:
                if self._is_healthy(obj):
                    return obj
                self._obj = None
                await self._close_obj(obj)
            self._obj = await self._factory()
            return self._obj

    async def release(self, obj: _T) -> None:
        """归还对象：单槽常驻共享，无操作"""

        pass

    async def discard(self, obj: _T) -> None:
        """丢弃当前对象并关闭；已被重建的对象不受影响"""

        async with self._lock:
            if self._obj is not obj:
                return None
            self._obj = None
        await self._close_obj(obj)

    async def close(self) -> None:
        """关闭持有的对象；幂等"""

        async with self._lock:
            if self._closed:
                return None
            self._closed = True
            obj = self._obj
            self._obj = None
        if obj is not None:
            await self._close_obj(obj)
        return None
