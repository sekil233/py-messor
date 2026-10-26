# coding: utf-8

import asyncio
import typing as t
from contextlib import asynccontextmanager


class IdleWaiter:

    __slots__ = (
        "_active_count",
        "_idle",
    )

    def __init__(self):
        self._active_count = 0
        self._idle = asyncio.Event()
        self._idle.set()

    async def wait(self) -> None:

        await self._idle.wait()

    @asynccontextmanager
    async def hold(self) -> t.AsyncIterator[None]:

        self._active_count += 1
        self._idle.clear()

        try:
            yield
        finally:
            self._active_count -= 1
            if self._active_count == 0:
                self._idle.set()
