# coding: utf-8
"""选择性启动：app.start(queues=[...]) 只启动部分消费者。

规则：
- 绑定的内存队列必须全部包含在 queues 中，否则报错
- 未选中的队列 consumer 不启动，但它的 producer 依然可用
  （app.stop() 时会按绑定顺序 stop/join 未启动队列的 producer）

需要本机 RabbitMQ，账号 guest/guest，端口 5672。队列不存在时会创建。

运行（项目根目录）:
    poetry run python examples/demo_partial_start.py
"""

import asyncio

from messor.factory import App, Backend

app = App()


@app.queue(Backend.QUEUE, name="task_a", consume_timeout=0.5)
async def task_a(job: str) -> None:

    print("a", job)
    await task_b.push(args=(job,))


@app.queue(Backend.QUEUE, name="task_b", consume_timeout=0.5)
async def task_b(job: str) -> None:

    print("b", job)


@app.queue(
    Backend.RABBIT_AIO_PIKA,
    name="task_r1",
    url="amqp://guest:guest@127.0.0.1:5672/",
    queue_name="demo_partial_r1",
    enable_declare_queue=True,
    queue_durable=True,
    consume_timeout=0.5,
)
async def task_r1(job: str) -> None:

    print("r1", job)


@app.queue(
    Backend.RABBIT_AIO_PIKA,
    name="task_r2",
    url="amqp://guest:guest@127.0.0.1:5672/",
    queue_name="demo_partial_r2",
    enable_declare_queue=True,
    queue_durable=True,
    consume_timeout=0.5,
)
async def task_r2(job: str) -> None:

    print("r2", job)


async def main() -> None:

    # 只启动 task_a、task_b（内存队列必须全选）和 task_r1；
    # task_r2 的 consumer 不启动
    await app.start(queues=["task_a", "task_b", "task_r1"])

    await task_a.push(args=("订单-001",))
    await task_r1.push(args=("订单-002",))
    await asyncio.sleep(1)

    # 未启动的 task_r2 的 producer 会被 app.stop() 一并收尾
    await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
