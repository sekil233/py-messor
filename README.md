# queue-func

[![Coverage Status](https://coveralls.io/repos/github/sekil9529/queue-func/badge.svg?branch=master)](https://coveralls.io/github/sekil9529/queue-func)
[![tests](https://github.com/sekil9529/queue-func/actions/workflows/tests.yml/badge.svg)](https://github.com/sekil9529/queue-func/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/queue-func.svg)](https://pypi.org/project/queue-func/)
[![Wheel](https://img.shields.io/pypi/wheel/queue-func.svg)](https://pypi.org/project/queue-func/)
[![Python Versions](https://img.shields.io/pypi/pyversions/queue-func.svg)](https://pypi.org/project/queue-func/)
[![License](https://img.shields.io/pypi/l/queue-func.svg)](https://pypi.org/project/queue-func/)

把一个 `async def` 收成队列任务：`push` 投递，消费者在同一个事件循环里执行。

仓库示例在 [examples/](examples/)。版本记录见 [CHANGELOG.md](CHANGELOG.md)。

> 需要 Python 3.12 及以上。任务必须是协程函数。
>
> RabbitMQ 后端是可选依赖，底层用 [aio-pika](https://github.com/mosquito/aio-pika) `^9.6`。

## 特点

- 进程内 `asyncio.Queue` 和 RabbitMQ 两种后端，装饰器都是 `@queue`。
- 每个任务有自己的队列、序列化和消费并发上限。
- `QueueFuncRuntime` 把多段任务按下游先启动、上游后停止的顺序一起管。
- `start` 会等到消费者开始收消息再返回。
- 装饰之后仍可以直接 `await` 调用，不经过队列。

## 启动和停止

`broker.start_consumer()` 创建消费任务，并等到它开始收消息。内存队列是已经堵在 `get()` 上；RabbitMQ 是队列已经订阅上。

`stop` 先标上停止，再等手头这次收消息结束。空闲时最多再等一个 `consume_timeout`。还没取走的 RabbitMQ 消息留在代理上。

RabbitMQ 在消息交给消费池之后确认。处理函数出错不会把消息退回队列。`push` 会等发布确认，超时抛出 `queue_func.PublishTimeoutError`。

## 安装

```shell
pip install queue-func
```

使用 RabbitMQ：

```shell
pip install "queue-func[rabbit]"
```

从仓库安装：

```shell
git clone https://github.com/sekil9529/queue-func.git
cd queue-func
poetry install -E rabbit
```

## 用法

从 `queue_func.factory` 引入 `queue`、`Backend`、`QueueFuncRuntime`。

### 进程内队列

```python
import asyncio

from queue_func.factory import Backend, queue


@queue(Backend.QUEUE, consume_timeout=0.5, maxsize=1000)
async def process_order(order_id: int) -> None:
    print(order_id)


async def main() -> None:
    await process_order.broker.start_consumer()
    await process_order.push(args=(42,))
    await process_order.broker.stop_consumer()
    await process_order.broker.join_consumer()


asyncio.run(main())
```

`push` 就是 `broker.publish`。不传 `timeout` 时用配置里的 `publish_timeout`；传入数字只对这一次有效。`None` 表示不限时。

直接调用：

```python
await process_order(1)
```

### 多段流水线

`QueueFuncRuntime.start()` 按列表反序启动，下游先就绪。`stop()` 先逐个停消费者，再停全部生产者。

```python
import asyncio

from queue_func.factory import Backend, QueueFuncRuntime, queue


@queue(Backend.QUEUE, consumer_pool_maxsize=4)
async def task_b(job: str) -> None:
    print(job)


@queue(Backend.QUEUE, consumer_pool_maxsize=2)
async def task_a(job: str) -> None:
    await task_b.push(args=(job,))


async def main() -> None:
    runtime = QueueFuncRuntime([task_a, task_b])
    await runtime.start()
    await task_a.push(args=("job-1",))
    await runtime.stop()


asyncio.run(main())
```

三段链式示例：

```shell
poetry run python examples/demo_queue.py
```

### RabbitMQ

`url` 和 `queue_name` 必填。`enable_declare_queue=True` 时，队列不存在会创建。默认交换机名是 `""`，路由键默认用队列名。已经存在的队列再声明时，持久化等参数必须和代理上的一致。

```python
import asyncio

from queue_func.factory import Backend, queue


@queue(
    Backend.RABBIT_AIO_PIKA,
    url="amqp://guest:guest@127.0.0.1/",
    queue_name="process_order",
    enable_declare_queue=True,
    consume_timeout=0.5,
)
async def process_order(order_id: int) -> None:
    print(order_id)


async def main() -> None:
    await process_order.broker.start_consumer()
    await process_order.push(args=(42,))
    await process_order.broker.stop_consumer()
    await process_order.broker.join_consumer()
    await process_order.broker.stop_producer()
    await process_order.broker.join_producer()


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

其余字段见 `queue_func.backends.queue.config.Config` 和 `queue_func.backends.rabbit_aio_pika.config.Config`。

## 一条任务里有什么

`@queue` 装饰的协程就是一条队列。`push` 投到它自己这条队列上。

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
