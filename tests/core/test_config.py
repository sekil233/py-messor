import pytest
from pydantic import ValidationError

from messor.core.config import Config
from messor.serializer.orjson_ser import Serializer as OrjsonSerializer
from messor.serializer.raw import Serializer as RawSerializer
from messor.pool_executor.general import PoolExecutor


async def noop():
    return None


def test_config_defaults():

    config = Config(coro=noop, name="q")

    assert config.coro is noop
    assert config.serializer_cls is OrjsonSerializer
    assert config.consume_timeout == 2.0
    assert config.consumer_pool_executor_cls is PoolExecutor
    assert config.consumer_pool_maxsize == 200
    assert config.publish_timeout is None
    assert config.name == "q"


def test_config_requires_coro():

    with pytest.raises(ValidationError):
        Config()


def test_config_requires_name():

    with pytest.raises(ValidationError):
        Config(coro=noop)


def test_config_custom_values():

    config = Config(
        coro=noop,
        name="q",
        serializer_cls=RawSerializer,
        consume_timeout=0.1,
        consumer_pool_maxsize=8,
        publish_timeout=3.5,
    )

    assert config.serializer_cls is RawSerializer
    assert config.consume_timeout == 0.1
    assert config.consumer_pool_maxsize == 8
    assert config.publish_timeout == 3.5
    assert config.name == "q"


def test_config_ignores_unknown_fields():

    config = Config(coro=noop, name="q", unknown=123)

    assert not hasattr(config, "unknown")


def test_config_is_frozen():

    config = Config(coro=noop, name="q")

    with pytest.raises(AttributeError):
        config.consume_timeout = 0.1
