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
