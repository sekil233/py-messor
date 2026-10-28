from messor.serializer.orjson_ser import Serializer as OrjsonSerializer
from messor.serializer.raw import Serializer
from messor.core.channel import BaseChannel


def test_base_channel_stores_serializer():

    channel = BaseChannel(Serializer)

    assert channel.serializer is Serializer


def test_base_channel_instances_keep_their_own_serializer():

    first = BaseChannel(Serializer)
    second = BaseChannel(OrjsonSerializer)

    assert first.serializer is Serializer
    assert second.serializer is OrjsonSerializer
