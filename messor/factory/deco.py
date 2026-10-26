# coding: utf-8

import typing as t

if t.TYPE_CHECKING:
    from messor.core.broker import BaseBroker

_P = t.ParamSpec("_P")
_R = t.TypeVar("_R")

__all__ = (
    "QueueFunc",
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
