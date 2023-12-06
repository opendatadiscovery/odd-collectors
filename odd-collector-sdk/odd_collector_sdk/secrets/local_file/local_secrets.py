from typing import Union
from pathlib import Path

from ..base_secrets import BaseSecretsBackend
from ...logger import logger
from ...errors import LoadConfigError
from ...utils.yaml_parser import parse_yaml


class LocalFileSecretsBackend(BaseSecretsBackend):
    def __init__(
        self,
        collector_config_path: Union[str, Path] = "collector_config.yaml",
        **kwargs
    ) -> None:
        super().__init__()
        self.collector_config_path = kwargs.get("collector_config_path", collector_config_path)

    def _read_config_file(self) -> dict:
        try:
            config_path = Path(self.collector_config_path).resolve()
            logger.debug(f"{config_path=}")
            logger.info("Start reading config")

            with open(config_path) as f:
                parsed = parse_yaml(f)

            return parsed
        except Exception as e:
            raise LoadConfigError(e)

    def get_platform_connection_settings(self) -> dict:
        config = self._read_config_file()
        config.pop("plugins", [])
        return config

    def get_plugins_settings(self) -> list[dict]:
        return self._read_config_file().pop("plugins", [])
