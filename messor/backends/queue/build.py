# coding: utf-8

import typing as t

from messor.factory.backend import Backend
from messor.factory.registry import register_backend

if t.TYPE_CHECKING:
    from messor.core.broker import BaseBroker


@register_backend(Backend.QUEUE)
def build_queue_broker(
        coro: t.Callable[..., t.Coroutine],
        **kwargs: t.Any) -> "BaseBroker[t.Any, t.Any]":

    from messor.backends.queue.broker import Broker
    from messor.backends.queue.config import Config

    return Broker(Config(coro=coro, **kwargs))
