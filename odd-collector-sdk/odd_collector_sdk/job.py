import asyncio
import traceback as tb
from contextlib import contextmanager
from datetime import timedelta
from inspect import isasyncgenfunction, iscoroutinefunction
from timeit import default_timer as timer
from typing import Any, Generator, Iterable, Union

from funcy import chunks
from odd_models.models import DataEntityList

from odd_collector_sdk.api.datasource_api import PlatformApi
from odd_collector_sdk.domain.adapter import Adapter

from .logger import logger


@contextmanager
def log_execution(name):
    try:
        start = timer()
        logger.info(f"[{name}] collecting metadata started.")
        yield
    except Exception as e:
        logger.debug(tb.format_exc())
        logger.error(f"[{name}] failed.\n {e}")
    else:
        end = timer()
        logger.success(
            f"[{name}] metadata collected in {timedelta(seconds=end - start)}."
        )


class AbstractJob:
    def __init__(self, api: PlatformApi, adapter: Adapter, chunk_size: int = 250):
        self._api = api
        self._adapter: Adapter = adapter
        self._chunk_size = chunk_size

    async def start() -> None:
        ...

    async def send_metadata(self, metadata: DataEntityList):
        await self._api.ingest_data(metadata)

    def _split(
        self, data_entity_lists: Union[DataEntityList, Iterable[DataEntityList]]
    ) -> Generator[DataEntityList, Any, Any]:
        if isinstance(data_entity_lists, DataEntityList):
            data_entity_lists = [data_entity_lists]

        for data_entity_list in data_entity_lists:
            for index, items in enumerate(
                chunks(self._chunk_size, data_entity_list.items), start=1
            ):
                logger.debug(
                    f"[{self._adapter.config.name}] Yield batch #{index} with {len(items)} items"
                )
                yield DataEntityList(
                    data_source_oddrn=self._adapter.get_data_source_oddrn(), items=items
                )


class AsyncJob(AbstractJob):
    async def start(self):
        with log_execution(self._adapter.config.name):
            tasks = []
            async for del_ in self._get_data_entity_list():
                task = asyncio.create_task(self.send_metadata(metadata=del_))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def _get_data_entity_list(self) -> Generator[DataEntityList, Any, Any]:
        data_entity_lists = await self._adapter.get_data_entity_list()
        for data_entity_list in self._split(data_entity_lists):
            yield data_entity_list


class SyncJob(AbstractJob):
    async def start(self):
        with log_execution(self._adapter.config.name):
            for del_ in self._get_data_entity_list():
                await self.send_metadata(metadata=del_)

    def _get_data_entity_list(self) -> Generator[DataEntityList, Any, Any]:
        data_entity_lists = self._adapter.get_data_entity_list()
        yield from self._split(data_entity_lists)


def create_job(api: PlatformApi, adapter: Adapter, chunk_size: int) -> AbstractJob:
    if isasyncgenfunction(adapter.get_data_entity_list):
        raise ValueError("Async generator is not supported.")
    if iscoroutinefunction(adapter.get_data_entity_list):
        logger.debug(f"Is async {adapter.config.name=}")
        return AsyncJob(api, adapter, chunk_size)
    else:
        return SyncJob(api, adapter, chunk_size)
