# coding: utf-8

import asyncio
import typing as t

from messor.serializer.base import BaseSerializer, Message


Resource = t.TypeVar("Resource", bound="BaseResource")


class BaseResource(t.Generic[Message]):

    __slots__ = (
        "_serializer",
        "_name",
    )

    def __init__(self, serializer_cls: type[BaseSerializer], name: str):

        self._serializer = serializer_cls
        self._name = name

    @property
    def serializer(self) -> type[BaseSerializer]:

        return self._serializer

    @property
    def name(self) -> str:
        """队列逻辑名"""

        return self._name
