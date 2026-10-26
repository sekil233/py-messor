# coding: utf-8
"""进程内链式队列：a 消费后 push 到 b，b 再 push 到 c。

运行（项目根目录）:
    poetry run python examples/demo_queue.py
"""

import asyncio

from queue_func.factory import App, Backend

app = App()


@app.queue(Backend.QUEUE, name="task_a", consume_timeout=0.5)
async def task_a(job: str) -> None:

    print("a", job)
    await task_b.push(args=(job,))


@app.queue(Backend.QUEUE, name="task_b", consume_timeout=0.5)
async def task_b(job: str) -> None:

    print("b", job)
    await task_c.push(args=(job,))


@app.queue(Backend.QUEUE, name="task_c", consume_timeout=0.5)
async def task_c(job: str) -> None:

    print("c", job)


async def main() -> None:

    await app.start()
    await task_a.push(args=("订单-001",))
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
