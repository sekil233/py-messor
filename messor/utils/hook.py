# coding: utf-8

import asyncio
import inspect
import typing as t
from enum import StrEnum, unique

__all__ = (
    "OnceHooks",
    "Hooks",
    "HookOrder",
)

_Hook = t.Callable[[], t.Awaitable[None] | None]


async def _invoke(fn: _Hook) -> None:

    result = fn()
    if inspect.isawaitable(result):
        await result


@unique
class HookOrder(StrEnum):

    FIFO = "fifo"
    LIFO = "lifo"


class OnceHooks:
    """一组回调，只启动一次。sync/async 均可。"""

    __slots__ = (
        "_hooks",
        "_task",
        "_lock",
        "_order",
    )

    def __init__(self, order: HookOrder = HookOrder.FIFO) -> None:
        self._hooks: list[_Hook] = []
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._order = order

    def register(self, hook: _Hook) -> _Hook:

        self._hooks.append(hook)
        return hook

    async def start(self, *, prepare: _Hook | None = None) -> None:
        """只启动"""

        async with self._lock:
            if self._task is None:
                if prepare is not None:
                    await _invoke(prepare)
                self._task = asyncio.create_task(self._run())

    async def wait(self) -> None:
        """只等待"""

        if self._task is not None:
            await self._task

    async def run(self, *, prepare: _Hook | None = None) -> None:
        """启动并等待"""

        await self.start(prepare=prepare)
        await self.wait()

    async def _run(self) -> None:

        hooks: t.Iterable[_Hook] = (
            reversed(self._hooks)
            if self._order is HookOrder.LIFO
            else self._hooks
        )
        for hook in hooks:
            await _invoke(hook)


class Hooks:
    """一组回调，可反复执行。每次 run 完成后重置内部状态。sync/async 均可。"""

    __slots__ = (
        "_hooks",
        "_task",
        "_order",
    )

    def __init__(self, order: HookOrder = HookOrder.FIFO) -> None:
        self._hooks: list[_Hook] = []
        self._order = order

    def register(self, hook: _Hook) -> _Hook:

        self._hooks.append(hook)
        return hook

    async def run(self, reset: bool = True) -> None:

        if not self._hooks:
            return None
        hooks = self._hooks if self._order == HookOrder.FIFO else self._hooks[::-1]
        for hook in hooks:
            await _invoke(hook)
        if reset:
            self._reset()
        return None

    def reset(self) -> None:

        self._reset()
        return None

    def _reset(self) -> None:

        self._hooks = []
        return None
