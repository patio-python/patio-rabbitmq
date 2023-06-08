PATIO Rabbitmq
==============

PATIO is an acronym for **P**ython **A**synchronous **T**ask for Async**IO**.

This package provides RabbitMQ broker implementation.

Example
-------

### Task executor

```python
import asyncio
import operator
from functools import reduce

from patio import Registry, ThreadPoolExecutor

from patio_rabbitmq import RabbitMQBroker


rpc = Registry(project="patio-rabbitmq", auto_naming=False)


@rpc("mul")
def mul(*args):
    return reduce(operator.mul, args)


async def main():
    async with ThreadPoolExecutor(rpc, max_workers=16) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            await broker.join()


if __name__ == "__main__":
    asyncio.run(main())
```

### Task producer

```python
import asyncio

from patio import NullExecutor, Registry

from patio_rabbitmq import RabbitMQBroker


async def main():
    async with NullExecutor(Registry(project="patio-rabbitmq")) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            print(
                await asyncio.gather(
                    *[
                        broker.call("mul", i, i, timeout=1) for i in range(10)
                    ]
                ),
            )


if __name__ == "__main__":
    asyncio.run(main())

```
