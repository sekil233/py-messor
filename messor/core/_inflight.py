# coding: utf-8

import asyncio

__all__ = (
    "inc",
    "dec",
    "wait_drained",
)


class _Inflight:
    """进程级全局在途消息计数（内存队列专用）。

    push 成功后 +1，coro 执行完成后 -1；
    计数归 0 表示无在途消息，可通过 wait_drained 等待该状态。
    """

    __slots__ = (
        "_count",
        "_event",
    )

    def __init__(self) -> None:

        self._count = 0
        self._event = asyncio.Event()
        self._event.set()

    def inc(self, n: int = 1) -> None:

        self._count += n
        # 有在途消息，阻塞 wait_drained
        self._event.clear()

    def dec(self, n: int = 1) -> None:

        self._count = max(0, self._count - n)
        if self._count == 0:
            # 无在途消息，放行 wait_drained
            self._event.set()

    async def wait_drained(self) -> None:

        while self._count > 0:
            await self._event.wait()

    def reset(self) -> None:
        """清空计数（测试隔离用，不对外暴露）。"""

        self._count = 0
        self._event.set()


# 全局单例：内存队列 producer/consumer 共享，不对外暴露
_INFLIGHT = _Inflight()


def inc(n: int = 1) -> None:
    """push 成功后 +n。"""

    _INFLIGHT.inc(n)


def dec(n: int = 1) -> None:
    """coro 执行完成后 -n（默认 -1，可传批量值）。"""

    _INFLIGHT.dec(n)


async def wait_drained() -> None:
    """等待在途消息全部处理完成。"""

    await _INFLIGHT.wait_drained()
