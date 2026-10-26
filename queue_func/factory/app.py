# coding: utf-8

import typing as t

from queue_func.core._inflight import wait_drained
from queue_func.factory import builders as _builders  # noqa: F401 — register backends
from queue_func.factory.backend import Backend
from queue_func.factory.deco import QueueFunc
from queue_func.factory.registry import build_broker

_P = t.ParamSpec("_P")
_R = t.TypeVar("_R")

__all__ = (
    "App",
)


class App:
    """类似 Celery 的应用：注册 func 队列，并按 name 控制消费者启停。

    用法::

        app = App()

        @app.queue(Backend.QUEUE, name="task_a", consume_timeout=0.5)
        async def task_a(job: str) -> None:
            ...

    - ``name`` 必填且唯一，仅作任务标识（rabbit 的 ``queue_name`` 独立传参）
    - ``app.start(queues=[name, ...])`` 只启动选中的消费者，不传则全部启动
    - 绑定的内存队列必须全部包含在选中名单中，否则报错
    - 启动顺序：内存队列 consumer 逆序启动，其余队列 consumer 逆序启动
    - ``stop`` 复用关闭编排：非内存 consumer 先停 -> wait_drained -> 内存 consumer -> producer
      ；绑定但未启动的 producer 按绑定顺序单独 stop/join
    """

    __slots__ = (
        "_funcs",
        "_started",
    )

    def __init__(self) -> None:

        self._funcs: dict[str, QueueFunc[t.Any, t.Any]] = {}
        self._started: list[str] = []

    def queue(
            self,
            backend: Backend,
            *,
            name: str,
            **config_kwargs: t.Any) -> t.Callable[[t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]], QueueFunc[_P, _R]]:

        if not name:
            raise ValueError("name 必填")
        if name in self._funcs:
            raise ValueError(f"name {name!r} 已注册")

        def decorator(fn: t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]) -> QueueFunc[_P, _R]:

            broker = build_broker(backend, fn, **config_kwargs)
            func = QueueFunc(fn, broker)
            self._funcs[name] = func
            return func

        return decorator

    def _select(self, queues: list[str] | None) -> list[tuple[str, QueueFunc[t.Any, t.Any]]]:

        if queues is None:
            return list(self._funcs.items())
        unknown = [name for name in queues if name not in self._funcs]
        if unknown:
            raise ValueError(f"未注册的队列名: {unknown}")
        return [(name, self._funcs[name]) for name in queues]

    async def start(self, queues: list[str] | None = None) -> None:

        selected = self._select(queues)
        selected_names = [name for name, _ in selected]

        # 绑定的内存队列必须全部包含在选中名单中
        for name, func in self._funcs.items():
            if func.broker.IS_MEMORY and name not in selected_names:
                raise ValueError(f"内存队列 {name!r} 必须包含在 queues 中")

        memory = [(name, func) for name, func in selected if func.broker.IS_MEMORY]
        others = [(name, func) for name, func in selected if not func.broker.IS_MEMORY]

        # 1. 内存队列 consumer 逆序启动
        for _, func in reversed(memory):
            await func.broker.start_consumer()
        # 2. 其余队列 consumer 逆序启动
        for _, func in reversed(others):
            await func.broker.start_consumer()

        self._started = selected_names

    async def stop(self) -> None:

        started = [self._funcs[name] for name in self._started]

        # 1. 非内存 consumer 逐个 stop → join
        for func in started:
            if not func.broker.IS_MEMORY:
                await func.broker.stop_consumer()
        for func in started:
            if not func.broker.IS_MEMORY:
                await func.broker.join_consumer()

        # 2. 等待在途消息全部处理完成
        await wait_drained()

        # 3. 内存 consumer 逐个 stop → join
        for func in started:
            if func.broker.IS_MEMORY:
                await func.broker.stop_consumer()
        for func in started:
            if func.broker.IS_MEMORY:
                await func.broker.join_consumer()

        # 4. 已启动的 producer 先全体 stop，再全体 join
        for func in started:
            await func.broker.stop_producer()
        for func in started:
            await func.broker.join_producer()

        # 5. 绑定但未启动的 producer 按绑定顺序 stop → join（消费逻辑可能 push 进这些队列）
        unstarted = [func for name, func in self._funcs.items() if name not in self._started]
        for func in unstarted:
            await func.broker.stop_producer()
        for func in unstarted:
            await func.broker.join_producer()

        self._started.clear()
