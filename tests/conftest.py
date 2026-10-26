# coding: utf-8

import pytest

from queue_func.core._inflight import _INFLIGHT


@pytest.fixture(autouse=True)
def _reset_inflight():
    """每个用例结束清空全局在途计数，避免用例间残留导致 wait_drained 挂起。"""

    yield
    _INFLIGHT.reset()
