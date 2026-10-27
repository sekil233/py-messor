# coding: utf-8

import asyncio

from messor.serializer.base import Message, BaseSerializer
from messor.core.resource import BaseResource as _Base


class Resource(_Base[Message]):

    __slots__ = (
        "_queue",
    )

    def __init__(self, serializer_cls: type[BaseSerializer], maxsize: int, name: str) -> None:

        super().__init__(serializer_cls, name)
        self._queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=maxsize)

    @property
    def queue(self) -> asyncio.Queue[Message]:

        return self._queue
