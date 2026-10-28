# coding: utf-8

import abc
import typing as t

from messor.serializer.base import Message
from messor.core.channel import Channel

if t.TYPE_CHECKING:
    from messor.core.config import Config
    from messor.core.producer import BaseProducer
    from messor.core.consumer import BaseConsumer


ConfigType = t.TypeVar("ConfigType", bound="Config")


class BaseBroker(t.Generic[ConfigType, Message], metaclass=abc.ABCMeta):
    """一条通道的装配者。

    创建时准备好双方共用的 Serializer，并持有配置与 Producer/Consumer。
    对外通过 publish、start_consumer 等入口操作；运行期业务逻辑仍在 Producer/Consumer 内完成。
    """

    __slots__ = (
        "_producer",
        "_consumer",
    )

    # 是否内存队列：内存队列需 wait_drained 后关闭，非内存队列可直接关
    IS_MEMORY = False

    _producer: "BaseProducer"
    _consumer: "BaseConsumer"

    async def publish(
            self,
            coro_args: tuple[t.Any, ...],
            coro_kwargs: dict[str, t.Any] | None = None,
            *,
            timeout: float | None = None) -> None:

        await self._producer.publish(coro_args, coro_kwargs, timeout=timeout)

    async def start_consumer(self) -> None:

        await self._consumer.consume()

    async def stop_consumer(self) -> None:

        await self._consumer.stop()

    async def join_consumer(self) -> None:

        await self._consumer.join()

    async def stop_producer(self) -> None:

        await self._producer.stop()

    async def join_producer(self) -> None:

        await self._producer.join()
