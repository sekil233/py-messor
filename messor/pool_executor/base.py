# coding: utf-8

import abc
import asyncio
import typing as t


class BasePoolExecutor(metaclass=abc.ABCMeta):

    __slots__ = (
        "_maxsize",
    )

    def __init__(self, maxsize: int):

        self._maxsize = maxsize

    @abc.abstractmethod
    async def submit(
            self,
            coro: t.Callable[..., t.Coroutine],
            coro_args: tuple[t.Any, ...],
            coro_kwargs: dict[str, t.Any],
            **kwargs) -> asyncio.Future:

        pass

    @abc.abstractmethod
    async def join(self) -> None:

        pass
