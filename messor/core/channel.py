# coding: utf-8

import typing as t

from messor.serializer.base import BaseSerializer, Message


Channel = t.TypeVar("Channel", bound="BaseChannel")


class BaseChannel(t.Generic[Message]):
    """一条队列两端（生产者/消费者）共享的传输通道"""

    __slots__ = (
        "_serializer",
    )

    def __init__(self, serializer_cls: type[BaseSerializer]):

        self._serializer = serializer_cls

    @property
    def serializer(self) -> type[BaseSerializer]:
        """两端必须使用同一编码"""

        return self._serializer
