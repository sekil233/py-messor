# coding: utf-8

import typing as t

from queue_func.core._inflight import wait_drained
from queue_func.factory.deco import QueueFunc

__all__ = (
    "QueueFuncRuntime",
)


class QueueFuncRuntime:
    """按固定顺序批量启动 / 关闭多个 :class:`QueueFunc`。

    关闭顺序：
    1. 非内存队列 consumer 逐个 stop → join（它们不依赖内存计数，可直接关）
    2. wait_drained（等在途消息全部处理完成）
    3. 内存队列 consumer 逐个 stop → join（此时队列已空，可安全关闭）
    4. producer 先全体 stop，再全体 join
    """

    __slots__ = (
        "_funcs",
    )

    def __init__(self, funcs: t.Iterable[QueueFunc[t.Any, t.Any]]) -> None:

        self._funcs = list(funcs)

    async def start(self) -> None:

        for func in reversed(self._funcs):
            await func.broker.start_consumer()

    async def stop(self) -> None:

        # 1. 非内存队列 consumer 逐个 stop → join
        for func in self._funcs:
            if not func.broker.IS_MEMORY:
                await func.broker.stop_consumer()
        for func in self._funcs:
            if not func.broker.IS_MEMORY:
                await func.broker.join_consumer()

        # 2. 等待在途消息全部处理完成
        await wait_drained()

        # 3. 内存队列 consumer 逐个 stop → join
        for func in self._funcs:
            if func.broker.IS_MEMORY:
                await func.broker.stop_consumer()
        for func in self._funcs:
            if func.broker.IS_MEMORY:
                await func.broker.join_consumer()

        # 4. producer 先全体 stop，再全体 join
        for func in self._funcs:
            await func.broker.stop_producer()
        for func in self._funcs:
            await func.broker.join_producer()
