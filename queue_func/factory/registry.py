# coding: utf-8

import importlib
import typing as t

from queue_func.factory.backend import Backend

if t.TYPE_CHECKING:
    from queue_func.core.broker import BaseBroker

__all__ = (
    "register_backend",
    "build_broker",
)

BrokerBuilder = t.Callable[..., "BaseBroker[t.Any, t.Any]"]

_BUILDERS: dict[Backend, BrokerBuilder] = {}

# 内置后端懒加载：首次 build 才导入对应模块，模块导入即自注册
_LAZY_BUILDERS: dict[Backend, str] = {
    Backend.QUEUE: "queue_func.backends.queue.build",
    Backend.RABBIT_AIO_PIKA: "queue_func.backends.rabbit_aio_pika.build",
}


def register_backend(backend: Backend) -> t.Callable[[BrokerBuilder], BrokerBuilder]:

    def decorator(fn: BrokerBuilder) -> BrokerBuilder:

        if backend in _BUILDERS:
            raise ValueError(f"backend {backend!r} is already registered")
        _BUILDERS[backend] = fn
        return fn

    return decorator


def build_broker(
        backend: Backend,
        coro: t.Callable[..., t.Coroutine],
        **kwargs: t.Any) -> "BaseBroker[t.Any, t.Any]":

    builder = _BUILDERS.get(backend)
    if builder is None:
        module_name = _LAZY_BUILDERS.get(backend)
        if module_name is not None:
            importlib.import_module(module_name)  # 导入即注册
            builder = _BUILDERS.get(backend)
    if builder is None:
        raise NotImplementedError(f"backend {backend!r} is not implemented yet")
    return builder(coro, **kwargs)
