# coding: utf-8

import typing as t
from pydantic.dataclasses import dataclass as pyd_dataclass

from messor.core.config import Config as _Config
from messor.core.config import ExtraIgnoreConfig
from messor.serializer.base import BaseSerializer
from messor.serializer.raw import Serializer


@pyd_dataclass(frozen=True, config=ExtraIgnoreConfig)
class Config(_Config):

    ## shared channel
    maxsize: int = 200
    serializer_cls: type[BaseSerializer] = Serializer
