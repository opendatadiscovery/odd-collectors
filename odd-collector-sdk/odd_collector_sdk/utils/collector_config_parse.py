from pathlib import Path
from typing import Union

from odd_collector_sdk.logger import logger
from odd_collector_sdk.errors import LoadConfigError
from odd_collector_sdk.types import PluginFactory
from odd_collector_sdk.utils.yaml_parser import parse_yaml
from odd_collector_sdk.domain.collector_config import CollectorConfig


def read_config_yaml(collector_config_path: Union[str, Path]) -> dict:
    """
    Read and parse a collector configuration .yaml file.

    Args:
        collector_config_path: an absolute path to the collector configuration file.

    Returns:
        Parsed configuration as a dictionary.
    """
    try:
        config_path = Path(collector_config_path).resolve()
        logger.debug(f"{config_path=}")
        logger.info("Start reading config")

        with open(config_path) as f:
            parsed = parse_yaml(f)

        return parsed
    except Exception as e:
        raise LoadConfigError(e)


def unpack_config_logical_sections(
    parsed_config: dict,
) -> tuple[dict, dict, list[dict]]:
    """
    Unpack information into a logical sections from a parsed configuration.

    Parameters:
        parsed_config: Parsed configuration as a dictionary.

    Returns:
        A tuple containing information about secrets backend and kwargs,
        collector settings, and plugins.
    """
    plugins = parsed_config.pop("plugins", [])
    secrets_info = {
        "secrets_backend": parsed_config.pop("secrets_backend", None),
        "secrets_backend_kwargs": parsed_config.pop("secrets_backend_kwargs", {}),
    }
    return secrets_info, parsed_config, plugins


def generate_collector_config(
    collector_settings: dict,
    plugins: list[dict],
    plugin_factory: PluginFactory,
) -> CollectorConfig:
    """
    Generate a CollectorConfig object from unparsed collector configuration (collector
    settings and information about all plugins).

    Parameters:
        collector_settings: platform connection settings.
        plugins: list of plugin configurations.
        plugin_factory: factory for choosing adapters for different plugin types.

    Returns:
        Custom objecet representing config structure.
    """
    config_dict = {
        **collector_settings,
        "plugins": [
            plugin_factory[plugin["type"]].parse_obj(plugin) for plugin in plugins
        ],
    }
    return CollectorConfig.parse_obj(config_dict)
