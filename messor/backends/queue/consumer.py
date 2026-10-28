# coding: utf-8

import asyncio

from messor.serializer.base import Message
from messor.backends.queue.channel import Channel
from messor.core._inflight import dec
from messor.core.consumer import BaseConsumer as _BaseConsumer


class Consumer(_BaseConsumer[Message, Channel]):

    __slots__ = ()

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        # coro 执行完成后 -1
        self._on_coro_done.register(dec)
    
    async def _consume_once(self) -> Message | None:

        queue = self._channel.queue
        try:
            message: Message = await asyncio.wait_for(
                queue.get(),
                timeout=self._consume_timeout,
            )
        except asyncio.TimeoutError:
            return None
        else:
            await self._dispatch(message)
            queue.task_done()
            return message

    async def _drain(self) -> None:

        queue = self._channel.queue
        while not queue.empty():
            message: Message = queue.get_nowait()
            await self._dispatch(message)
            queue.task_done()

    async def _on_shutdown(self) -> None:

        await self._channel.queue.join()
        await self._pool_executor.obj.join()
