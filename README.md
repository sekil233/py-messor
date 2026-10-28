# py-messor

[![Coverage Status](https://coveralls.io/repos/github/sekil233/py-messor/badge.svg?branch=master)](https://coveralls.io/github/sekil233/py-messor)
[![tests](https://github.com/sekil233/py-messor/actions/workflows/tests.yml/badge.svg)](https://github.com/sekil233/py-messor/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/py-messor.svg)](https://pypi.org/project/py-messor/)
[![Wheel](https://img.shields.io/pypi/wheel/py-messor.svg)](https://pypi.org/project/py-messor/)
[![Python Versions](https://img.shields.io/pypi/pyversions/py-messor.svg)](https://pypi.org/project/py-messor/)
[![License](https://img.shields.io/pypi/l/py-messor.svg)](https://pypi.org/project/py-messor/)

将一个 `async def` 装饰成 `Queue`：支持 `push` 投递。

> 需要 Python 3.12 及以上。任务必须是协程函数。

## py-messor中的`Queue`是什么?

`queue` 是 py-messor 对「一条通道」的抽象：它底层可以是 `asyncio.Queue`、RabbitMQ、Redis 等，按需选择对应 `backend`。

一条 `queue` 的运行链路：`push` 经由 **Producer** 写入 **Channel**（通道），**Consumer** 取出后交给 **PoolExecutor**（消费池）按并发上限调用你的协程；多条 `queue` 由 **App** 统一启停。

## 特点

- **天然异步**：整个调用链都是 `async def`，与 `asyncio` 原生融合，全程无阻塞调用。
- **高性能**：存储级后端生产端池化并发 `push`，消费端消费池并发执行。
- **简单**：一个 `@app.queue` 装饰器即可，无需额外配置。

  ```python
  from messor.factory import App, Backend

  app = App()

  @app.queue(Backend.QUEUE, name="hello")
  async def hello(name: str) -> None:
      print(f"hello, {name}")
  ```
- **灵活**：
  - **自由投递**：任意队列的 `push` 可互相调用，内存队列与非内存队列可自由混用成链。
  - **后端可选**：内存级（`asyncio.Queue`）与存储级（如 RabbitMQ）按需选择对应 `backend`。
  - **队列级定制**：每个 `queue` 独立配置 `backend`、`serializer_cls`、`consumer_pool_executor_cls`、`consumer_pool_maxsize` 等，互不影响。

## 安装

```shell
pip install py-messor
```

### 支持的 Backend

| Backend | Queue | 安装方式 |
|---------|------|----------|
| `Backend.QUEUE` | asyncio.Queue | 无需额外安装 |
| `Backend.RABBIT_AIO_PIKA` | RabbitMQ | pip install "py-messor[rabbit]" |

## 用法

从 `messor.factory` 引入 `App`、`Backend`。`name` 必填且唯一，用来在 `start(queues=[...])` 里点名。

### 示例

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

直接调用：

```python
await process_order(1)
```

更多场景见 [examples/](examples/)：

| 示例 | 场景 |
|------|------|
| `demo_queue.py` | 进程内链式队列（a → b → c） |
| `demo_partial_start.py` | 选择性启动，内存 + RabbitMQ 混用 |
| `demo_rabbit_aio_pika.py` | RabbitMQ 队列 |
| `demo_network.py` | 混合队列网络（内存 + RabbitMQ 回环） |

## 限流

每个 `queue` 可独立配置消费速率，相关参数均在 `@app.queue(..., **kwargs)` 上配置：

- `rate_limit`：消费速率 `<n>/<unit>`，如 `100/s`、`50/m`、`10/h`、`5/d`；`None`（默认）不限流。
- `rate_limiter`：算法，`RateLimiter.TOKEN_BUCKET`（默认）、`RateLimiter.FIXED_WINDOW`、`RateLimiter.LEAKY_BUCKET`。
- `rate_limit_storage`：计数存储，当前仅内置 `RateLimitStorage.MEMORY`（进程内存），多实例各自计数；后续将支持 Redis 等持久化存储，实现跨实例全局限流。
- `rate_limit_key`：限流桶 key，默认取队列逻辑名、前缀 `messor:rate:`（如 `messor:rate:process_order`）；可覆盖，共享同一存储时自定义作用域。

### 示例

```python
@app.queue(Backend.QUEUE, name="process_order", rate_limit="100/s")
async def process_order(order_id: int) -> None:
    print(order_id)
```

> 注意：内存存储的计数在进程重启后清零，多实例部署下各自计数、不满足全局限流；`/h`、`/d` 等长窗口会整窗驻留内存。此时代码会发出 `UserWarning`，建议改用持久化存储。

## 生产者池

用于并发push提速。内存级Backend不适用（如`Backend.QUEUE`）。

- `producer_pool_maxsize`：发布 channel 池大小，默认 `3`，并发 `push` 至多开出这么多 channel；`None` 表示不池化，单 channel 常驻复用。

## 消费池`PoolExecutor`

用于按并发上限执行消费协程，插件式，可替换实现。

- `consumer_pool_executor_cls`：消费池类，默认 `PoolExecutor`；可继承 `BasePoolExecutor` 自定义。
- `consumer_pool_maxsize`：消费并发上限，默认 `200`。

<!--
## 内部结构

`@app.queue` 装饰的协程就是一条队列。`push` 投到它自己这条队列上。

- **Broker** 组装生产者、消费者和双方共享的 Channel（传输通道），并提供启停与投递入口。
- **Producer** 序列化后把数据写入队列。
- **Serializer** 处理入队出队的序列化操作。
- **Consumer** 取出数据，反序列化后交给消费池。
- **PoolExecutor** 按并发上限执行业务协程。

### 一次`push`

```mermaid
flowchart TB
  PUSH["await f.push(...)"] -> B["Broker"]
  B -> PRO["Producer"]
  PRO ->|Serializer.pack| Q[("Channel")]
  Q ->|Serializer.unpack| CON["Consumer"]
  CON ->|submit| POOL["PoolExecutor"]
  POOL ->|call| FN["await f(...)"]
```
-->

## 测试

```shell
poetry install -E rabbit
poetry run pytest
```

## 版本

变更记录: [CHANGELOG.md](CHANGELOG.md)。

## 许可证

Apache-2.0
