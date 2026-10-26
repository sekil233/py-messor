# coding: utf-8

import asyncio

from queue_func.errors import PublishTimeoutError
from queue_func.serializer.base import Message
from queue_func.core._inflight import inc
from queue_func.core.producer import BaseProducer
from queue_func.backends.queue.resource import Resource


class Producer(BaseProducer[Message, Resource]):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        # push 成功后 +1
        self._on_publish_success.register(inc)

    async def _publish(self, msg: Message, *, timeout: float | None) -> None:

        if timeout:
            try:
                await asyncio.wait_for(self._resource.queue.put(msg), timeout)
            except asyncio.TimeoutError as exc:
                raise PublishTimeoutError("publish timed out") from exc
            return None
        await self._resource.queue.put(msg)
