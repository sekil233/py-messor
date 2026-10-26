# coding: utf-8

from enum import StrEnum, unique

__all__ = (
    "Backend",
)


@unique
class Backend(StrEnum):

    QUEUE = "queue"
    RABBIT_AIO_PIKA = "rabbit_aio_pika"
