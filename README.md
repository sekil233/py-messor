# messor

[![Coverage Status](https://coveralls.io/repos/github/sekil9529/py-messor/badge.svg?branch=master)](https://coveralls.io/github/sekil9529/py-messor)
[![tests](https://github.com/sekil9529/py-messor/actions/workflows/tests.yml/badge.svg)](https://github.com/sekil9529/py-messor/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/py-messor.svg)](https://pypi.org/project/py-messor/)
[![Wheel](https://img.shields.io/pypi/wheel/py-messor.svg)](https://pypi.org/project/py-messor/)
[![Python Versions](https://img.shields.io/pypi/pyversions/py-messor.svg)](https://pypi.org/project/py-messor/)
[![License](https://img.shields.io/pypi/l/py-messor.svg)](https://pypi.org/project/py-messor/)

把一个 `async def` 收成队列任务：`push` 投递，消费者在同一个事件循环里执行。

仓库示例在 [examples/](examples/)。版本记录见 [CHANGELOG.md](CHANGELOG.md)。

> 需要 Python 3.12 及以上。任务必须是协程函数。
>
> RabbitMQ 后端是可选依赖，底层用 [aio-pika](https://github.com/mosquito/aio-pika) `^9.6`。

## 特点

- 进程内 `asyncio.Queue` 和 RabbitMQ 两种后端，统一用 `App` 的 `@app.queue` 装饰。
- 每个任务有自己的队列、序列化和消费并发上限。
- `App.start()` 逆序启动消费者（下游先就绪），`App.stop()` 按关闭编排回收。
- `start(queues=[...])` 可以只启动部分消费者；绑定的内存队列必须全部包含，否则报错。
- `start` 会等到消费者开始收消息再返回。
- 装饰之后仍可以直接 `await` 调用，不经过队列。

## 启动和停止

`app.start()` 启动消费者：内存队列 consumer 先按逆序启动，其余队列随后也按逆序启动（下游先就绪）。`start` 会等到消费者开始收消息再返回——内存队列已经堵在 `get()` 上，RabbitMQ 是队列已经订阅上。

`app.stop()` 按编排关闭：

1. 非内存队列 consumer 逐个 stop/join——先断掉往内存队列投递的外部源；
2. `wait_drained` 等全部在途消息处理完成（此时内存 consumer 仍在消费）；
3. 内存队列 consumer 逐个 stop/join；
4. 生产者全部 stop/join；绑定但未启动的 producer 也按绑定顺序一并收尾。

单个 consumer 的 stop 先标上停止，再等手头这次收消息结束；空闲时最多再等一个 `consume_timeout`。还没取走的 RabbitMQ 消息留在代理上。

RabbitMQ 在消息交给消费池之后确认。处理函数出错不会把消息退回队列。`push` 会等发布确认，超时抛出 `messor.PublishTimeoutError`。

## 安装

```shell
pip install py-messor
```

使用 RabbitMQ：

```shell
pip install "py-messor[rabbit]"
```

从仓库安装：

```shell
git clone https://github.com/sekil9529/py-messor.git
cd py-messor
poetry install -E rabbit
```

## 用法

从 `messor.factory` 引入 `App`、`Backend`。`name` 必填且唯一，用来在 `start(queues=[...])` 里点名。

### 进程内队列

```python
import asyncio

from messor.factory import App, Backend

app = App()


@app.queue(Backend.QUEUE, name="process_order", consume_timeout=0.5, maxsize=1000)
async def process_order(order_id: int) -> None:
    print(order_id)


async def main() -> None:
    await app.start()
    await process_order.push(args=(42,))
    await app.stop()


asyncio.run(main())
```

`push` 就是 `broker.publish`。不传 `timeout` 时用配置里的 `publish_timeout`；传入数字只对这一次有效。`None` 表示不限时。

直接调用：

```python
await process_order(1)
```

### 多段流水线

`App.start()` 逆序启动消费者：内存队列 consumer 先按逆序启动，其余队列随后也按逆序启动，下游先就绪。`stop()` 复用关闭编排：非内存 consumer 先停 -> wait_drained -> 内存 consumer -> producer。

```python
import asyncio

from messor.factory import App, Backend

app = App()


@app.queue(Backend.QUEUE, name="task_b", consumer_pool_maxsize=4)
async def task_b(job: str) -> None:
    print(job)


@app.queue(Backend.QUEUE, name="task_a", consumer_pool_maxsize=2)
async def task_a(job: str) -> None:
    await task_b.push(args=(job,))


async def main() -> None:
    await app.start()
    await task_a.push(args=("job-1",))
    await app.stop()


asyncio.run(main())
```

三段链式示例：

```shell
poetry run python examples/demo_queue.py
```

### 只启动部分队列

`app.start(queues=[name, ...])` 只启动点名的那几个消费者，其余队列的 consumer 不启动。不传 `queues` 则全部启动。

- 绑定的内存队列必须全部包含在 `queues` 里，否则报错（内存队列没有独立持久化，漏启会导致投递无处消费）。
- 未启动队列的 producer 依然可用，`app.stop()` 收尾时会按绑定顺序把它一并 stop/join。

示例（内存 `task_a`/`task_b` 必须全选，rabbit 只启动 `task_r1`）：

```python
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
    consume_timeout=0.5,
)
async def task_r1(job: str) -> None:
    print("r1", job)


async def main() -> None:
    await app.start(queues=["task_a", "task_b", "task_r1"])
    await task_a.push(args=("订单-001",))
    await asyncio.sleep(1)
    await app.stop()


asyncio.run(main())
```

完整示例：

```shell
poetry run python examples/demo_partial_start.py
```

### RabbitMQ

`url` 和 `queue_name` 必填。`enable_declare_queue=True` 时，队列不存在会创建。默认交换机名是 `""`，路由键默认用队列名。已经存在的队列再声明时，持久化等参数必须和代理上的一致。

```python
import asyncio

from messor.factory import App, Backend

app = App()


@app.queue(
    Backend.RABBIT_AIO_PIKA,
    name="process_order",
    url="amqp://guest:guest@127.0.0.1/",
    queue_name="process_order",
    enable_declare_queue=True,
    consume_timeout=0.5,
)
async def process_order(order_id: int) -> None:
    print(order_id)


async def main() -> None:
    await app.start()
    await process_order.push(args=(42,))
    await app.stop()


asyncio.run(main())
```

本地 RabbitMQ（`127.0.0.1:5672`，`guest` / `guest`）上的链式示例：

```shell
poetry run python examples/demo_rabbit_aio_pika.py
```

常用参数：

| 参数 | 作用 |
|------|------|
| `consumer_pool_maxsize` | 同时执行的协程数，默认 200 |
| `consume_timeout` | 一次等消息的时间，默认 2 秒 |
| `publish_timeout` | 投递等待上限，默认不限 |
| `maxsize` | 仅内存队列，`asyncio.Queue` 容量，默认 200 |
| `serializer_cls` | 队列中的数据结构。内存队列默认 `raw`，RabbitMQ 默认 orjson |
| `prefetch_count` | 仅 RabbitMQ，未确认消息窗口 |
| `delivery_mode` | 仅 RabbitMQ，默认持久化 |
| `exchange_name` / `routing_key` | 仅 RabbitMQ |
| `queue_durable` | 仅 RabbitMQ，声明队列时是否持久化，默认否 |

其余字段见 `messor.backends.queue.config.Config` 和 `messor.backends.rabbit_aio_pika.config.Config`。

## 一条任务里有什么

`@app.queue` 装饰的协程就是一条队列。`push` 投到它自己这条队列上。

- **Broker** 组装生产者、消费者和双方共用的资源，并提供启停与投递。
- **Producer** 序列化后把数据写入队列。
- **Consumer** 取出数据，反序列化后交给消费池。
- **PoolExecutor** 按并发上限执行业务协程。

```mermaid
flowchart TB
  PUSH["投递"] --> B["通道"]
  B --> PRO["生产者"]
  B -.->|启停| CON["消费者"]

  PRO -->|序列化| Q[("队列")]
  Q -->|反序列化| CON
  CON --> POOL["消费池"]
  POOL --> FN["业务协程"]
```

## 测试

```shell
poetry install -E rabbit
poetry run pytest
```

单元测试使用假的 RabbitMQ 连接，不要求本机有代理。`examples/demo_rabbit_aio_pika.py` 需要本机 `guest` / `guest`、端口 `5672`。

`pytest.ini` 会写出覆盖率页面 `htmlcov/` 和 `report.html`。

## 版本

遵循[语义化版本](https://semver.org/lang/zh-CN/)。变更记录在 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

Apache-2.0
