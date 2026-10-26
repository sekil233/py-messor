# coding: utf-8
import asyncio
import time


class Deadline:
    """可刷新的截止时间点，支持异步等待到期。

    - reset()  刷新截止时间为 now + duration，并叫醒正在等待的协程重新计时
    - wait()   等到截止时间点到期，或被 wakeup() 提前打断
    """

    __slots__ = (
        "_duration",
        "_expires_at",
    )

    def __init__(self, duration: float | int):
        self._duration = duration
        self._expires_at = time.monotonic() + duration

    def reset(self) -> None:
        self._expires_at = time.monotonic() + self._duration

    def is_expired(self) -> bool:
        return time.monotonic() >= self._expires_at

    def remaining(self) -> float:

        return max(self._expires_at - time.monotonic(), 0.0)

    async def wait(self) -> None:

        await asyncio.sleep(self.remaining())
