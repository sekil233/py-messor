# coding: utf-8

import typing as t

from .base import BaseTask

if t.TYPE_CHECKING:
    from concurrent.futures import Future


class ThreadTask(BaseTask):

    def __init__(self, fut: "Future"):
        self._fut = fut

    def running(self) -> bool:

        return self._fut.running()

    def done(self) -> bool:

        return self._fut.done()

    def cancel(self) -> bool:

        return self._fut.cancel()
