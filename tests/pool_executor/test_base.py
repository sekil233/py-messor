# coding: utf-8

import pytest

from messor.pool_executor.base import BasePoolExecutor


def test_base_pool_executor_cannot_be_instantiated():

    with pytest.raises(TypeError):
        BasePoolExecutor(1)


def test_base_pool_executor_stores_maxsize():

    class Dummy(BasePoolExecutor):

        async def submit(self, coro, coro_args, coro_kwargs, **kwargs):
            return None

        async def join(self):
            return None

    executor = Dummy(4)
    assert executor._maxsize == 4
