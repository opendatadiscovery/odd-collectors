import os
from pathlib import Path
from typing import Dict, List, Optional, Type, Union

import pydantic

from ..errors import LoadConfigError
from ..logger import logger
from ..utils.yaml_parser import parse_yaml as parse_config
from .plugin import Plugin


class CollectorConfig(pydantic.BaseSettings):
    default_pulling_interval: Optional[int] = None  # minutes
    connection_timeout_seconds: int = 300
    token: str
    plugins: List[Plugin]
    platform_host_url: str
    chunk_size: int = 250
    misfire_grace_time: Optional[
        int
    ]  # seconds after the designated runtime that the job is still allowed to be run
    max_instances: Optional[
        int
    ] = 1  # maximum number of concurrently running instances allowed
    verify_ssl: bool = True


def load_config(
    config_path: Union[str, Path], plugin_factory: Dict[str, Type[Plugin]]
) -> CollectorConfig:
    config_path = config_path or os.getenv("CONFIG_PATH", "collector_config.yaml")

    try:
        config_path = Path(config_path).resolve()
        logger.debug(f"{config_path=}")
        logger.info("Start reading config")

        with open(config_path) as f:
            parsed = parse_config(f)
            parsed["plugins"] = [
                plugin_factory[plugin["type"]].parse_obj(plugin)
                for plugin in parsed["plugins"]
            ]

            return CollectorConfig.parse_obj(parsed)
    except Exception as e:
        raise LoadConfigError(e)
