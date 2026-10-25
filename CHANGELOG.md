0.2.4
-----

* refactor: 消费循环、启动等待与关闭顺序收进 `core` 基类，后端只实现 `_on_startup`、`_consume_once`、`_drain`。`@queue` 用法不变

0.2.3
-----

* refactor: 后端实现收到 `backends/`，共用基类收到 `core`。`@queue` 用法不变
* fix: 消费者开始收消息后才算启动完成，避免还没启动就被 `stop`
* docs: README 与示例收成可直接运行的短链

0.2.2
-----

* docs: README 增加 PyPI、测试与覆盖率徽章
* ci: 新增 GitHub Actions 测试，并把覆盖率上传到 Coveralls

0.2.1
-----

* feat: 新增 `Backend.RABBIT_AIO_PIKA`，通过可选依赖 aio-pika `^9.6` 接入 RabbitMQ

0.1.2
-----

* feat: 合并 `message` 与 `serializer`，并支持 orjson
* feat: `push` 支持按次 `timeout` 与 `publish_timeout`，超时抛出 `PublishTimeoutError`

0.1.0
-----

* feat: 新增 `@queue`、`QueueFunc` 与进程内 `Backend.QUEUE`
* feat: 新增 `QueueFuncRuntime`，按固定顺序批量启停多个 `QueueFunc`
* feat: 新增消费池并发上限 `consumer_pool_maxsize`
* feat: 新增 `register_backend`，便于接入新的队列后端
* docs: 新增 README 与 `examples/demo_queue.py`
* test: 新增 pytest，覆盖 broker、producer、consumer、serializer、装饰器与 runtime
* chore: 包名定为 queue-func，要求 Python 3.12+
