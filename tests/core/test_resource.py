from queue_func.serializer.orjson_ser import Serializer as OrjsonSerializer
from queue_func.serializer.raw import Serializer
from queue_func.core.resource import BaseResource


def test_base_resource_stores_serializer():

    resource = BaseResource(Serializer)

    assert resource.serializer is Serializer


def test_base_resource_instances_keep_their_own_serializer():

    first = BaseResource(Serializer)
    second = BaseResource(OrjsonSerializer)

    assert first.serializer is Serializer
    assert second.serializer is OrjsonSerializer
