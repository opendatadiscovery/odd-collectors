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
        collector_config_path (Union[str, Path]): an absolute path to the collector configuration file.

    Returns:
        dict: Parsed configuration as a dictionary.
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
        parsed_config (dict): Parsed configuration as a dictionary.

    Returns:
        tuple[dict, dict, list[dict]]: A tuple containing information about secrets backend and kwargs,
            platform connection settings, and plugins.
    """
    plugins = parsed_config.pop("plugins", [])
    secrets_info = {
        "secrets_backend": parsed_config.pop("secrets_backend", None),
        "secrets_backend_kwargs": parsed_config.pop("secrets_backend_kwargs", {}),
    }
    return secrets_info, parsed_config, plugins


def generate_collector_config(
    platform_connection_settings: dict,
    plugins: list[dict],
    plugin_factory: PluginFactory,
) -> CollectorConfig:
    """
    Generate a CollectorConfig object from unparsed collector configuration (platform connection
    settings and information about all plugins).

    Parameters:
        platform_connection_settings (dict): Platform connection settings.
        plugins (list[dict]): List of plugin configurations.
        plugin_factory (PluginFactory): Factory for choosing adapters for different plugin types.

    Returns:
        CollectorConfig: custom objecet representing config structure.
    """
    config_dict = {
        **platform_connection_settings,
        "plugins": [
            plugin_factory[plugin["type"]].parse_obj(plugin) for plugin in plugins
        ],
    }
    return CollectorConfig.parse_obj(config_dict)
