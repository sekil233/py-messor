# coding: utf-8

from enum import unique, StrEnum, IntEnum

from pydantic.dataclasses import dataclass as pyd_dataclass

from messor.core.config import Config as _Config
from messor.core.config import ExtraIgnoreConfig


@unique
class ExchangeName(StrEnum):

    DEFAULT = ""
    DIRECT = "amq.direct"
    TOPIC = "amq.topic"
    FANOUT = "amq.fanout"
    HEADERS = "amq.headers"


@unique
class DeliveryMode(IntEnum):

    NOT_PERSISTENT = 1
    PERSISTENT = 2


@pyd_dataclass(frozen=True, config=ExtraIgnoreConfig, kw_only=True)
class Config(_Config):

    ## shared resource
    # amqp://user:password@host/vhost?heartbeat=60&x=y
    url: str
    queue_name: str
    enable_declare_queue: bool = False
    # broker 重启后队列仍在；消息是否保留看 delivery_mode
    queue_durable: bool = False
    # 仅当前连接可使用，连接断开后队列删除
    queue_exclusive: bool = False
    # 不创建队列，只确认同名队列已存在，不存在则失败
    queue_passive: bool = False
    # 最后一个消费者取消或断开后删除队列
    queue_auto_delete: bool = False
    # 声明时的附加参数，例如 x-message-ttl、x-dead-letter-exchange
    queue_arguments: dict | None = None

    ## producer
    exchange_name: ExchangeName | str = ExchangeName.DEFAULT
    routing_key: str = ""
    delivery_mode: DeliveryMode = DeliveryMode.PERSISTENT

    ## consumer
    prefetch_count: int = 0
    prefetch_size: int = 0
    global_qos: bool = False
