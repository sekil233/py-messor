# coding: utf-8

import typing as t

from queue_func.backends.rabbit_aio_pika.config import DeliveryMode, ExchangeName
from queue_func.factory import builders as _builders  # noqa: F401 — register backends
from queue_func.factory.backend import Backend
from queue_func.factory.registry import build_broker

if t.TYPE_CHECKING:
    from queue_func.core.broker import BaseBroker
    from queue_func.core.config import Config as GeneralConfig
    from queue_func.pool_executor.base import BasePoolExecutor
    from queue_func.serializer.base import BaseSerializer

_P = t.ParamSpec("_P")
_R = t.TypeVar("_R")

__all__ = (
    "QueueFunc",
    "queue",
)


class QueueFunc(t.Generic[_P, _R]):
    """可调用协程，并附带 push。"""

    def __init__(
            self,
            fn: t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]],
            broker: "BaseBroker[t.Any, t.Any]") -> None:

        self._fn = fn
        self._broker = broker
        self.__wrapped__ = fn
        self.__name__ = fn.__name__
        self.__qualname__ = fn.__qualname__
        self.__doc__ = fn.__doc__
        self.__annotations__ = getattr(fn, "__annotations__", {})

    async def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _R:

        return await self._fn(*args, **kwargs)

    async def push(
            self,
            *,
            args: tuple[t.Any, ...] = (),
            kwargs: dict[str, t.Any] | None = None,
            timeout: float | None = None) -> None:

        await self._broker.publish(coro_args=args, coro_kwargs=kwargs, timeout=timeout)

    @property
    def broker(self) -> "BaseBroker[t.Any, t.Any]":

        return self._broker


def _queue_impl(
        backend: Backend,
        **config_kwargs: t.Any) -> t.Callable[[t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]], QueueFunc[_P, _R]]:

    def decorator(fn: t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]) -> QueueFunc[_P, _R]:

        broker = build_broker(backend, fn, **config_kwargs)
        return QueueFunc(fn, broker)

    return decorator


@t.overload
def queue(
        backend: t.Literal[Backend.QUEUE],
        *,
        consume_timeout: float = ...,
        consumer_pool_executor_cls: type["BasePoolExecutor"] = ...,
        consumer_pool_maxsize: int = ...,
        serializer_cls: type["BaseSerializer"] = ...,
        publish_timeout: float | None = ...,
        maxsize: int = ...,
) -> t.Callable[[t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]], QueueFunc[_P, _R]]:

    ...


@t.overload
def queue(
        backend: t.Literal[Backend.RABBIT_AIO_PIKA],
        *,
        url: str,
        queue_name: str,
        enable_declare_queue: bool = ...,
        queue_durable: bool = ...,
        queue_exclusive: bool = ...,
        queue_passive: bool = ...,
        queue_auto_delete: bool = ...,
        queue_arguments: dict[str, t.Any] | None = ...,
        serializer_cls: type["BaseSerializer[bytes]"] = ...,
        consume_timeout: float = ...,
        consumer_pool_executor_cls: type["BasePoolExecutor"] = ...,
        consumer_pool_maxsize: int = ...,
        prefetch_count: int = ...,
        prefetch_size: int = ...,
        global_qos: bool = ...,
        exchange_name: ExchangeName | str = ...,
        routing_key: str = ...,
        delivery_mode: DeliveryMode = ...,
        publish_timeout: float | None = ...,
) -> t.Callable[[t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]], QueueFunc[_P, _R]]:

    ...


def queue(
        backend: Backend,
        **config_kwargs: t.Any) -> t.Callable[[t.Callable[_P, t.Coroutine[t.Any, t.Any, _R]]], QueueFunc[_P, _R]]:

    return _queue_impl(backend, **config_kwargs)
