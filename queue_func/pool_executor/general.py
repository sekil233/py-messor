# coding: utf-8

import asyncio
import typing as t

from queue_func.pool_executor.base import BasePoolExecutor
from queue_func.utils.sem import JoinableSemaphore


class PoolExecutor(BasePoolExecutor):

    __slots__ = (
        "_sem",
    )

    def __init__(self, maxsize: int):
        super().__init__(maxsize)
        self._sem = JoinableSemaphore(self._maxsize)

    async def submit(
            self,
            coro: t.Callable[..., t.Coroutine],
            coro_args: tuple[t.Any, ...],
            coro_kwargs: dict[str, t.Any],
            **kwargs) -> asyncio.Future:

        await self._sem.acquire()
        return asyncio.gather(self._consume(coro, coro_args, coro_kwargs))

    async def _consume(
            self,
            coro: t.Callable[..., t.Coroutine],
            coro_args: tuple[t.Any, ...],
            coro_kwargs: dict[str, t.Any]) -> None:
        try:
            await coro(*coro_args, **coro_kwargs)
        finally:
            self._sem.release()

    async def join(self) -> None:

        await self._sem.join()
