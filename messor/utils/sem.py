# coding: utf-8

import asyncio


class JoinableSemaphore:

    __slots__ = (
        "_maxsize",
        "_count",
        "_sem",
        "_event",
    )

    def __init__(self, maxsize: int):

        self._maxsize = maxsize
        self._count = 0
        self._sem = asyncio.Semaphore(self._maxsize)
        self._event = self._init_event()

    @staticmethod
    def _init_event() -> "asyncio.Event":

        event = asyncio.Event()
        event.set()
        return event

    async def acquire(self) -> bool:

        is_ok = await self._sem.acquire()
        if is_ok:
            self._count += 1
            # block until set
            self._event.clear()
        return is_ok

    def release(self) -> None:

        self._sem.release()
        self._count = max(0, self._count - 1)
        if self._count == 0:
            # not block at all
            self._event.set()

    async def join(self) -> None:

        while self._count > 0:
            await self._event.wait()
