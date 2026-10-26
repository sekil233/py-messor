# coding: utf-8

import pytest

from messor.factory import Backend
from messor.factory import registry


def test_build_broker_lazy_imports_backend_module(monkeypatch):

    imports: list[str] = []
    monkeypatch.setattr(registry, "_BUILDERS", {})

    def fake_import(name: str, *args, **kwargs):
        imports.append(name)
        return None

    monkeypatch.setattr(registry.importlib, "import_module", fake_import)

    async def fn():
        return None

    with pytest.raises(NotImplementedError):
        registry.build_broker(Backend.QUEUE, fn)

    # 空注册表下首次 build 一定发起后端模块的导入（懒加载）
    assert imports == ["messor.backends.queue.build"]


def test_build_broker_unknown_backend_raises(monkeypatch):

    monkeypatch.setattr(registry, "_BUILDERS", {})
    monkeypatch.setattr(registry, "_LAZY_BUILDERS", {})

    async def fn():
        return None

    with pytest.raises(NotImplementedError):
        registry.build_broker("none", fn)
