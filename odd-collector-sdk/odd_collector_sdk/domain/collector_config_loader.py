import os
from pathlib import Path
from typing import Dict, Optional, Type, Union

from odd_collector_sdk.utils.yaml_parser import parse_yaml

from ..errors import LoadConfigError
from ..logger import logger
from ..secrets.secrets_backend_factory import (
    SecretsBackendFactory,
    SecretsBackendSettings,
)
from .collector_config import CollectorConfig
from .plugin import Plugin


class CollectorConfigLoader:
    def __init__(
        self, config_path: Union[str, Path], plugin_factory: Dict[str, Type[Plugin]]
    ) -> None:
        self.plugin_factory = plugin_factory

        config_path = config_path or os.getenv("CONFIG_PATH", "collector_config.yaml")
        self.path = Path(config_path).resolve()

    def load(self) -> CollectorConfig:
        logger.debug(f"Config path {self.path}")
        logger.info("Start reading config")

        try:
            return self._build_collector_config()
        except Exception as e:
            raise LoadConfigError(e)

    def _build_collector_config(self):
        conf_dict = self._parse_config()
        plugins = conf_dict.pop("plugins", [])
        secrets_backend: Optional[dict] = conf_dict.pop("secrets_backend", None)
        collector_settings = conf_dict

        if secrets_backend is not None:
            sb_provider = SecretsBackendFactory(
                SecretsBackendSettings(**secrets_backend)
            ).get_provider()
            collector_settings = self._merge_collector_settings(
                sb_provider.get_collector_settings(), collector_settings
            )
            plugins = self._merge_plugins(sb_provider.get_plugins(), plugins)

        plugins = [
            self.plugin_factory[plugin["type"]].model_validate(plugin)
            for plugin in plugins
        ]

        return CollectorConfig.model_validate(
            {**collector_settings, "plugins": plugins}
        )

    def _parse_config(self) -> dict:
        """
        Read and parse a collector configuration .yaml file.

        Returns:
            Parsed configuration as a dictionary.
        """
        with open(self.path) as stream:
            logger.debug("Parsing config")
            parsed = parse_yaml(stream)

        return parsed

    @staticmethod
    def _merge_plugins(secret_backend_plugins: list[dict], local_plugins: list[dict]):
        merged_plugins = secret_backend_plugins.copy()

        # names are unique, we can't have 2 and more different plugins with one name
        priority_plugins_names = [p["name"] for p in secret_backend_plugins]

        for plugin in local_plugins:
            if plugin["name"] not in priority_plugins_names:
                merged_plugins.append(plugin)

        return merged_plugins

    @staticmethod
    def _merge_collector_settings(
        secret_backend_collector_settings: dict, local_collector_settings: dict
    ):
        """
        Merge collector settings from two sources, giving priority to priority_settings.

        Parameters:
            secret_backend_collector_settings: the collector settings from a higher-priority source.
            local_collector_settings: the collector settings from a lower-priority source.

        Returns:
            Merged collector settings with priority given to priority_settings.
        """
        merged_settings = secret_backend_collector_settings.copy()

        for key, value in local_collector_settings.items():
            if key not in secret_backend_collector_settings:
                merged_settings[key] = value

        return merged_settings
