# coding: utf-8

import functools
import typing as t

_S = t.TypeVar("_S")
_P = t.ParamSpec("_P")
_R = t.TypeVar("_R")

type Prepare = t.Callable[[t.Any], t.Awaitable[t.Any]]


def before(
        prepare: Prepare,
) -> t.Callable[
    [t.Callable[t.Concatenate[_S, _P], t.Awaitable[_R]]],
    t.Callable[t.Concatenate[_S, _P], t.Awaitable[_R]],
]:
    """Run ``prepare`` before the wrapped method.

    ``prepare`` is an async callable that receives ``self``.
    """

    def decorate(
            method: t.Callable[t.Concatenate[_S, _P], t.Awaitable[_R]],
    ) -> t.Callable[t.Concatenate[_S, _P], t.Awaitable[_R]]:

        @functools.wraps(method)
        async def wrapper(self: _S, *args: _P.args, **kwargs: _P.kwargs) -> _R:

            await prepare(self)
            return await method(self, *args, **kwargs)

        return wrapper

    return decorate
