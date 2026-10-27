# coding: utf-8


class FakeClock:
    """虚拟时钟：替换 time.monotonic / time.monotonic_ns / asyncio.sleep，
    让限流测试的时间推进即时完成，不真实等待。"""

    def __init__(self, start: float = 0.0) -> None:

        self._now = start

    @property
    def now(self) -> float:

        return self._now

    def advance(self, seconds: float) -> None:

        self._now += seconds

    def monotonic(self) -> float:

        return self._now

    def monotonic_ns(self) -> int:

        return int(self._now * 1_000_000_000)

    async def sleep(self, seconds: float) -> None:

        self.advance(seconds)


def wire_clock(clock: FakeClock, mock_sleep, mock_monotonic, mock_monotonic_ns) -> None:
    """把 FakeClock 接到三个 patch.object mock 的 side_effect 上"""

    mock_sleep.side_effect = clock.sleep
    mock_monotonic.side_effect = clock.monotonic
    mock_monotonic_ns.side_effect = clock.monotonic_ns
