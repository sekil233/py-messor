# coding: utf-8

import asyncio
import typing as t

from messor.serializer.base import BaseSerializer, Message


Resource = t.TypeVar("Resource", bound="BaseResource")


class BaseResource(t.Generic[Message]):

    __slots__ = (
        "_serializer",
    )

    def __init__(self, serializer_cls: type[BaseSerializer]):

        self._serializer = serializer_cls

    @property
    def serializer(self) -> type[BaseSerializer]:

        return self._serializer
