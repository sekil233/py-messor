# coding: utf-8

import asyncio
import typing as t

from queue_func.serializer.base import Message
from queue_func.backends.queue.resource import Resource
from queue_func.pool_executor.base import BasePoolExecutor
from queue_func.core.consumer import BaseConsumer as _BaseConsumer


class Consumer(_BaseConsumer[Message, Resource]):


    async def _consume_once(self) -> Message | None:

        queue = self._resource.queue
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

        queue = self._resource.queue
        while not queue.empty():
            message: Message = queue.get_nowait()
            await self._dispatch(message)
            queue.task_done()

    async def _on_shutdown(self) -> None:

        await self._resource.queue.join()
        await self._pool_executor.obj.join()
