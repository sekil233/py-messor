# coding: utf-8

import typing as t

from messor.factory.backend import Backend
from messor.factory.registry import register_backend

if t.TYPE_CHECKING:
    from messor.core.broker import BaseBroker


@register_backend(Backend.RABBIT_AIO_PIKA)
def build_rabbit_aio_pika_broker(
        coro: t.Callable[..., t.Coroutine],
        **kwargs: t.Any) -> "BaseBroker[t.Any, t.Any]":

    try:
        from messor.backends.rabbit_aio_pika.broker import Broker
        from messor.backends.rabbit_aio_pika.config import Config
    except ImportError as exc:
        raise ImportError(
            "Backend.RABBIT_AIO_PIKA requires aio-pika. "
            "Install it with the rabbit extra.",
        ) from exc

    return Broker(Config(coro=coro, **kwargs))
