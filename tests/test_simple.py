import operator
import os
from contextlib import AsyncExitStack
from functools import reduce
from typing import Any, AsyncGenerator, Callable, Union

import pytest
from patio import NullExecutor, Registry, ThreadPoolExecutor

from patio_rabbitmq import RabbitMQBroker


rpc: Registry[Callable[..., Any]] = Registry(project="test")


@rpc("mul")
def mul(*args: Union[int, float]) -> Union[int, float]:
    return reduce(operator.mul, args)


@rpc("div")
def div(*args: Union[int, float]) -> Union[int, float]:
    return reduce(operator.truediv, args)


AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@localhost/")


@pytest.fixture()
async def thread_executor() -> AsyncGenerator[Any, ThreadPoolExecutor]:
    async with ThreadPoolExecutor(rpc) as executor:
        yield executor


async def test_simple(thread_executor: ThreadPoolExecutor):
    async with RabbitMQBroker(
        thread_executor, amqp_url=AMQP_URL, auto_delete=True,
    ) as broker:
        assert await broker.call("mul", 1, 2, 3, 4, 5) == 120
        assert await broker.call(mul, 1, 2, 3, 4, 5) == 120

        with pytest.raises(ZeroDivisionError):
            assert await broker.call(div, 1, 0)


async def test_simple_client_server(thread_executor: ThreadPoolExecutor):
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            RabbitMQBroker(
                thread_executor, amqp_url=AMQP_URL, auto_delete=True,
            ),
        )

        broker = await stack.enter_async_context(
            RabbitMQBroker(
                NullExecutor(Registry(rpc.project)),
                amqp_url=AMQP_URL,
                auto_delete=True,
            ),
        )

        assert await broker.call("mul", 1, 2, 3, 4, 5) == 120

        with pytest.raises(ZeroDivisionError):
            assert await broker.call("div", 1, 0)


async def test_close_twice(thread_executor: ThreadPoolExecutor):
    broker = RabbitMQBroker(thread_executor, amqp_url=AMQP_URL)
    await broker.setup()
    await broker.close()
    await broker.close()


async def test_fail_when_no_setup(thread_executor: ThreadPoolExecutor):
    broker = RabbitMQBroker(thread_executor, amqp_url=AMQP_URL)
    with pytest.raises(RuntimeError):
        assert await broker.call("mul", 1, 2, 3, 4, 5) == 120
