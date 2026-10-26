# coding: utf-8

from messor.serializer.orjson_ser import Serializer


def test_orjson_serializer_roundtrip():

    packed = Serializer.pack((1, "a"), {"c": 3})

    assert isinstance(packed, bytes)
    assert Serializer.unpack(packed) == ((1, "a"), {"c": 3})


def test_orjson_serializer_pack_fills_empty_kwargs():

    assert Serializer.unpack(Serializer.pack((1,))) == ((1,), {})
