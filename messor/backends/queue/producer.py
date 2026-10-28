# coding: utf-8

import asyncio

from messor.errors import PublishTimeoutError
from messor.serializer.base import Message
from messor.core._inflight import inc
from messor.core.producer import BaseProducer
from messor.backends.queue.channel import Channel


class Producer(BaseProducer[Message, Channel]):

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)
        # push 成功后 +1
        self._on_publish_success.register(inc)

    async def _publish(self, msg: Message, *, timeout: float | None) -> None:

        if timeout:
            try:
                await asyncio.wait_for(self._channel.queue.put(msg), timeout)
            except asyncio.TimeoutError as exc:
                raise PublishTimeoutError("publish timed out") from exc
            return None
        await self._channel.queue.put(msg)
