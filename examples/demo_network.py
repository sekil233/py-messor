# coding: utf-8
"""混合队列网络：3 个内存队列 + 1 个 RabbitMQ，验证有限自投递 + 有限投递。

投递链: a -> r -> b -> c -> r（rabbit 位于 a 与 b 之间）。
hop 从 0 递增，每次投递 +1，hop >= MAX_HOP 停止投递，网络收敛。
- task_a (QUEUE): 先投递 task_r，sleep(1) 后自投递
- task_r (RABBIT): 先自投递，再投递 task_b
- task_b (QUEUE): 先自投递，sleep(3) 后投递 task_c
- task_c (QUEUE): 先自投递，再投递 task_r（回环）

测试目的:
- 混合后端下有限自投递 + 有限投递（含 rabbit 位于投递链中段）
- 关闭编排：非内存 consumer 先 stop/join -> wait_drained -> 内存 consumer -> producer

需要本机 RabbitMQ，账号 guest/guest，端口 5672。队列不存在时会创建。

运行（项目根目录）:
    poetry run python examples/demo_network.py
"""

import asyncio

from queue_func.factory import Backend, QueueFuncRuntime, queue

MAX_HOP = 3


@queue(Backend.QUEUE, consume_timeout=0.5)
async def task_a(job: str, hop: int) -> None:

    print("a", job, hop)
    if hop >= MAX_HOP:
        return None
    await task_r.push(args=(f"r:{job}", hop + 1))
    await asyncio.sleep(1)
    await task_a.push(args=(f"a:{job}", hop + 1))


@queue(
    Backend.RABBIT_AIO_PIKA,
    url="amqp://guest:guest@127.0.0.1:5672/",
    queue_name="demo_network_r",
    enable_declare_queue=True,
    queue_durable=True,
    consume_timeout=0.5,
)
async def task_r(job: str, hop: int) -> None:

    print("r", job, hop)
    if hop >= MAX_HOP:
        return None
    await task_r.push(args=(f"r:{job}", hop + 1))
    await task_b.push(args=(f"b:{job}", hop + 1))


@queue(Backend.QUEUE, consume_timeout=0.5)
async def task_b(job: str, hop: int) -> None:

    print("b", job, hop)
    if hop >= MAX_HOP:
        return None
    await task_b.push(args=(f"b:{job}", hop + 1))
    await asyncio.sleep(3)
    await task_c.push(args=(f"c:{job}", hop + 1))


@queue(Backend.QUEUE, consume_timeout=0.5)
async def task_c(job: str, hop: int) -> None:

    print("c", job, hop)
    if hop >= MAX_HOP:
        return None
    await task_c.push(args=(f"c:{job}", hop + 1))
    await task_r.push(args=(f"r:{job}", hop + 1))


async def main() -> None:

    runtime = QueueFuncRuntime([task_a, task_b, task_c, task_r])
    await runtime.start()
    await task_a.push(args=("root", 0))
    await asyncio.sleep(0.5)
    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
