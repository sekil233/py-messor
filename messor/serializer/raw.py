# coding: utf-8

import typing as t

from messor.serializer.base import CoroParams, BaseSerializer


class Serializer(BaseSerializer[CoroParams]):

    __slots__ = ()

    @staticmethod
    def pack(
            coro_args: tuple[t.Any],
            coro_kwargs: dict[str, t.Any] | None = None) -> CoroParams:

        if coro_kwargs is None:
            coro_kwargs = {}
        return coro_args, coro_kwargs

    @staticmethod
    def unpack(msg: CoroParams) -> CoroParams:

        return msg
