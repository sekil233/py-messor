# coding: utf-8

import pytest

from messor.utils.deco import before

from tests.helpers import run_async


class _Box:

    def __init__(self) -> None:

        self.steps: list[str] = []

    async def _prepare(self) -> None:

        self.steps.append("prepare")

    @before(_prepare)
    async def run(self, value: int, *, label: str) -> str:

        self.steps.append(f"run {value} {label}")
        return label

    @before(_prepare)
    async def fail(self) -> None:

        self.steps.append("fail")
        raise RuntimeError("boom")


async def _prepare_error(obj: _Box) -> None:

    obj.steps.append("prepare")
    raise RuntimeError("not ready")


class _Guarded:

    def __init__(self) -> None:

        self.steps: list[str] = []

    @before(_prepare_error)
    async def run(self) -> None:

        self.steps.append("run")


@run_async
async def test_before_runs_prepare_then_method_and_returns():

    box = _Box()

    result = await box.run(3, label="ok")

    assert result == "ok"
    assert box.steps == ["prepare", "run 3 ok"]


@run_async
async def test_before_runs_prepare_on_every_call():

    box = _Box()

    await box.run(1, label="a")
    await box.run(2, label="b")

    assert box.steps == ["prepare", "run 1 a", "prepare", "run 2 b"]


@run_async
async def test_before_prepare_error_skips_method():

    guarded = _Guarded()

    with pytest.raises(RuntimeError, match="not ready"):
        await guarded.run()

    assert guarded.steps == ["prepare"]


@run_async
async def test_before_method_error_happens_after_prepare():

    box = _Box()

    with pytest.raises(RuntimeError, match="boom"):
        await box.fail()

    assert box.steps == ["prepare", "fail"]
