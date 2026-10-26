# coding: utf-8

import typing as t

import orjson

from messor.serializer.base import BaseSerializer, CoroParams


class Serializer(BaseSerializer[bytes]):

    __slots__ = ()

    @staticmethod
    def pack(
            coro_args: tuple[t.Any],
            coro_kwargs: dict[str, t.Any] | None = None) -> bytes:

        if coro_kwargs is None:
            coro_kwargs = {}
        return orjson.dumps((coro_args, coro_kwargs))

    @staticmethod
    def unpack(msg: bytes) -> CoroParams:

        args, kwargs = orjson.loads(msg)
        return tuple(args), kwargs
