# coding: utf-8

import threading
import typing as t

_T = t.TypeVar("_T")
_P = t.ParamSpec("_P")


class LazyObject(t.Generic[_T]):

    __slots__ = (
        "_call",
        "_args",
        "_kwargs",
        "_lock",
        "_obj",
    )

    def __init__(self, call: t.Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> None:
        self._call = call
        self._args = args
        self._kwargs = kwargs
        self._obj: _T | None = None
        self._lock = threading.Lock()

    @property
    def obj(self) -> _T:

        return self.get_or_create()[0]

    def get_or_create(self) -> tuple[_T, bool]:
        """获取或新建"""

        created = False
        if self._obj is not None:
            return self._obj, created
        with self._lock:
            if self._obj is None:
                created = True
                self._obj = self._call(*self._args, **self._kwargs)
        return self._obj, created

    def is_created(self) -> bool:

        return self._obj is not None
