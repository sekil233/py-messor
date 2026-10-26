# coding: utf-8

import abc
import typing as t

Message = t.TypeVar("Message")

type CoroParams = tuple[tuple[t.Any], dict[str, t.Any]]


class BaseSerializer(t.Generic[Message], metaclass=abc.ABCMeta):

    __slots__ = ()

    @staticmethod
    @abc.abstractmethod
    def pack(
            coro_args: tuple[t.Any],
            coro_kwargs: dict[str, t.Any] | None = None) -> Message:

        pass

    @staticmethod
    @abc.abstractmethod
    def unpack(msg: Message) -> CoroParams:

        pass
