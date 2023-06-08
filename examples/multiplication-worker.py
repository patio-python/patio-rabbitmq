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
