# coding: utf-8

from messor.factory import App, Backend

from tests.helpers import run_async
from tests.rabbit_aio_pika_fakes import FakeWorld, patch_connect, wait_until


@run_async
async def test_app_queue_publishes_to_rabbit_consumer():

    received: list[int] = []
    app = App()

    @app.queue(
        Backend.RABBIT_AIO_PIKA,
        name="work",
        url="amqp://guest:guest@127.0.0.1/",
        queue_name="jobs",
        consume_timeout=0.05,
        consumer_pool_maxsize=2,
    )
    async def work(n: int):
        received.append(n)

    world = FakeWorld()

    with patch_connect(world):
        await app.start()
        await work.push(args=(9,))
        await wait_until(lambda: received == [9])

        world.queue.force_timeout = True
        world.queue.event.set()
        await app.stop()

    assert received == [9]
    assert world.connection.closed is True
