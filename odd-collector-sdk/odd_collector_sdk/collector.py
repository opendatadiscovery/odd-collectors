import asyncio
import logging
import signal
import traceback
from asyncio import AbstractEventLoop
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import tzlocal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from odd_models.models import DataSource, DataSourceList

from odd_collector_sdk.domain.adapter import Adapter
from odd_collector_sdk.job import create_job
from odd_collector_sdk.logger import logger
from odd_collector_sdk.shutdown import shutdown, shutdown_by
from odd_collector_sdk.types import PluginFactory

from .api.datasource_api import PlatformApi
from .api.http_client import HttpClient
from .domain.collector_config import load_config
from .errors import PlatformApiError
from .load_adapter import load_adapters
from .utils.print_version import print_collector_packages_info

logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)


class Collector:
    """All ODD collectors should use that class to run.

    Attributes:
        config_path: Path| str
            Path to "collector_config.yaml" file
        root_package: str
            Package name for derived collector
        plugin_factory: dict
            fabric for plugins
        plugins_package: str:
            subpackage where plugins are stored.

    Example:
        >>> collector = Collector(
            config_path=Path().cwd() / "collector_config.yaml",
            root_package="odd_collector",
            plugin_factory=PLUGIN_FACTORY,
        )
        >>>collector.run()

    """

    _adapters: List[Adapter]

    def __init__(
        self,
        config_path: Union[str, Path],
        root_package: str,
        plugin_factory: PluginFactory,
        plugins_package: str = "adapters",
    ) -> None:
        print_collector_packages_info(root_package)
        self.config = load_config(config_path, plugin_factory)

        self._adapters = load_adapters(
            f"{root_package}.{plugins_package}", self.config.plugins
        )
        self._api = PlatformApi(
            http_client=HttpClient(
                token=self.config.token,
                connection_timeout_seconds=self.config.connection_timeout_seconds,
                verify_ssl=self.config.verify_ssl,
            ),
            platform_url=self.config.platform_host_url,
        )

    def start_polling(self):
        misfire_grace_time = (
            self.config.misfire_grace_time or self.config.default_pulling_interval * 60
        )

        scheduler = AsyncIOScheduler(timezone=str(tzlocal.get_localzone()))
        for adapter in self._adapters:
            scheduler.add_job(
                create_job(self._api, adapter, self.config.chunk_size).start,
                "interval",
                minutes=self.config.default_pulling_interval,
                next_run_time=datetime.now(),
                misfire_grace_time=misfire_grace_time,
                max_instances=self.config.max_instances,
                coalesce=True,
                id=adapter.config.name,
            )
        scheduler.start()

    async def register_data_sources(self):
        data_sources: List[DataSource] = [
            DataSource(
                oddrn=adapter.get_data_source_oddrn(),
                name=adapter.config.name,
                description=adapter.config.description,
            )
            for adapter in self._adapters
        ]

        await self._api.register_datasource(DataSourceList(items=data_sources))

    async def one_time_run(self):
        tasks = [
            asyncio.create_task(
                create_job(self._api, adapter, self.config.chunk_size).start()
            )
            for adapter in self._adapters
        ]

        await asyncio.gather(*tasks)

    def run(self, loop: Optional[AbstractEventLoop] = None):
        if not loop:
            loop = asyncio.get_event_loop()
        try:
            signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
            for s in signals:
                loop.add_signal_handler(
                    s, lambda s=s: asyncio.create_task(shutdown_by(s, loop))
                )

            loop.run_until_complete(self.register_data_sources())

            interval = self.config.default_pulling_interval
            logger.info(f"Config interval {interval=}")
            if not interval:
                logger.info("Collector will be run once.")
                loop.run_until_complete(self.one_time_run())
            else:
                self.start_polling()
                loop.run_forever()
        except PlatformApiError as e:
            logger.error(e)
            if e.request:
                logger.debug(e.request)
            loop.run_until_complete(shutdown(loop))
        except Exception as e:
            logger.debug(traceback.format_exc())
            logger.error(e)
            loop.run_until_complete(shutdown(loop))
