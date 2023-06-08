from functools import partial
from math import ceil
from typing import Any, Dict, Optional, Tuple, Union

import aio_pika.abc
from aio_pika import connect_robust
from aio_pika.patterns.rpc import RPC
from patio import AbstractExecutor, TaskFunctionType
from patio.broker import AbstractBroker, TimeoutType
from yarl import URL


class RabbitMQBroker(AbstractBroker):
    def __init__(
        self,
        executor: AbstractExecutor, *,
        amqp_url: Union[str, URL], **rpc_kwargs: Any
    ):
        super().__init__(executor)
        self.__amqp_url = URL(amqp_url)
        self.__rpc_kwargs = rpc_kwargs
        self.__connection: Optional[aio_pika.abc.AbstractConnection] = None
        self.__rpc: Optional[RPC] = None

    def execute(
        self, name: str, *, args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Any:
        return self.executor.execute(name, *args, **kwargs)

    async def setup(self) -> None:
        self.__connection = await connect_robust(self.__amqp_url)

        channel = await self.__connection.channel()
        await channel.set_qos(self.executor.max_workers)
        self.__rpc = await RPC.create(channel)

        for name, func in self.executor.registry.items():
            await self.__rpc.register(
                name, partial(self.execute, name),
                **self.__rpc_kwargs,
            )

    @property
    def _rpc(self) -> RPC:
        if self.__rpc is None:
            raise RuntimeError(f"{self.__class__.__name__} has not been setup")
        return self.__rpc

    async def call(
        self, func: Union[str, TaskFunctionType], *args: Any,
        timeout: Optional[TimeoutType] = None, **kwargs: Any
    ) -> Any:
        if not isinstance(func, str):
            func = self.executor.registry.get_name(func)

        return await self._rpc.call(
            func,
            kwargs=dict(args=args, kwargs=kwargs),
            expiration=ceil(timeout) if timeout else None,
        )

    async def close(self) -> None:
        if self.__connection is None:
            return
        await self.__connection.close()
        self.__connection = None
        await super().close()
