import asyncio
import logging
import signal
import traceback
from importlib import import_module
from asyncio import AbstractEventLoop
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import tzlocal
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from odd_models.models import DataSource, DataSourceList
from odd_collector_sdk.job import create_job
from odd_collector_sdk.logger import logger
from odd_collector_sdk.shutdown import shutdown, shutdown_by
from odd_collector_sdk.types import PluginFactory
from odd_collector_sdk.errors import PlatformApiError
from odd_collector_sdk.load_adapter import load_adapters
from odd_collector_sdk.domain.adapter import Adapter
from odd_collector_sdk.api.datasource_api import PlatformApi
from odd_collector_sdk.api.http_client import HttpClient
from odd_collector_sdk.utils.print_version import print_collector_packages_info
from odd_collector_sdk.utils.collector_config_generation import (
    read_config_yaml,
    unpack_config_logical_sections,
    generate_collector_config,
    merge_collector_settings,
    merge_plugins,
)
from odd_collector_sdk.secrets.mappers.backend_paths import (
    BACKEND_NAME_PATH_MAPPING,
)


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
            config_path="/absolute/path/to/collector_config.yaml",
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

        # Parse collector_config.yaml
        parsed_config = read_config_yaml(config_path)
        (
            secrets_backend_info,
            local_collector_settings,
            local_plugins,
        ) = unpack_config_logical_sections(parsed_config)

        secrets_backend_provider = secrets_backend_info["secrets_backend_provider"]
        secrets_backend_kwargs = secrets_backend_info["secrets_backend_kwargs"]

        # Dynamically import the secrets backend class if it is provided
        secret_backend_collector_settings, secret_backend_plugins = {}, []
        if secrets_backend_provider:
            (
                secrets_backend_module,
                secrets_backend_class_name,
            ) = BACKEND_NAME_PATH_MAPPING[secrets_backend_provider].rsplit(".", 1)
            try:
                secrets_backend_module = import_module(secrets_backend_module)
                secrets_backend_class = getattr(
                    secrets_backend_module, secrets_backend_class_name
                )
            except ImportError as e:
                error_message = f"Could not import '{secrets_backend_provider}'"
                raise ImportError(error_message) from e

            # Retrieve collector config secreats from backend
            secrets_backend_class_instance = secrets_backend_class(
                **secrets_backend_kwargs
            )
            secret_backend_collector_settings = (
                secrets_backend_class_instance.get_collector_settings()
            )
            secret_backend_plugins = secrets_backend_class_instance.get_plugins()

        # Merge config from local and secret backend sources
        merged_collector_settings = merge_collector_settings(
            secret_backend_collector_settings, local_collector_settings
        )
        merged_plugins = merge_plugins(secret_backend_plugins, local_plugins)

        self.config = generate_collector_config(
            merged_collector_settings, merged_plugins, plugin_factory
        )
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
