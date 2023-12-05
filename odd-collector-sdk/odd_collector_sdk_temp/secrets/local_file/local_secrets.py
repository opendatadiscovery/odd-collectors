from typing import Union
from pathlib import Path

from ..base_secrets import BaseSecretsBackend
from ...types import PluginFactory
from ...domain.collector_config import CollectorConfig, load_config


class LocalFileSecretsBackend(BaseSecretsBackend):
    def __init__(
        self,
        collector_config_path: Union[str, Path],
        plugin_factory: PluginFactory
    ) -> None:
        super().__init__()
        self.collector_config_path = collector_config_path
        self.plugin_factory = plugin_factory

    def get_collector_config(self) -> CollectorConfig:
        return load_config(self.collector_config_path, self.plugin_factory)
