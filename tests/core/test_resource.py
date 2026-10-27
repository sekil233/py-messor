from messor.serializer.orjson_ser import Serializer as OrjsonSerializer
from messor.serializer.raw import Serializer
from messor.core.resource import BaseResource


def test_base_resource_stores_serializer():

    resource = BaseResource(Serializer, "q")

    assert resource.serializer is Serializer


def test_base_resource_stores_name():

    resource = BaseResource(Serializer, "q")

    assert resource.name == "q"


def test_base_resource_instances_keep_their_own_serializer():

    first = BaseResource(Serializer, "q")
    second = BaseResource(OrjsonSerializer, "w")

    assert first.serializer is Serializer
    assert second.serializer is OrjsonSerializer
