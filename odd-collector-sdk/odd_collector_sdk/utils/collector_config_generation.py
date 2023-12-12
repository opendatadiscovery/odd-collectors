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
    config = parsed_config.copy()

    plugins = config.pop("plugins", [])
    secrets_backend_info = config.pop("secrets_backend", {})
    parsed_secrets_backend_info = {
        "secrets_backend_provider": secrets_backend_info.pop("provider", None),
        "secrets_backend_kwargs": secrets_backend_info,
    }
    return parsed_secrets_backend_info, config, plugins


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
        Custom CollectorConfig objecet representing config structure.
    """
    config_dict = {
        **collector_settings,
        "plugins": [
            plugin_factory[plugin["type"]].parse_obj(plugin) for plugin in plugins
        ],
    }
    return CollectorConfig.parse_obj(config_dict)


def merge_collector_settings(priority_settings: dict, settings: dict) -> dict:
    """
    Merge collector settings from two sources, giving priority to priority_settings.

    Parameters:
        priority_settings: the collector settings from a higher-priority source.
        settings: the collector settings from a lower-priority source.

    Returns:
        Merged collector settings with priority given to priority_settings.
    """
    merged_settings = priority_settings.copy()

    for key, value in settings.items():
        if key not in priority_settings:
            merged_settings[key] = value

    return merged_settings


def merge_plugins(priority_plugins: list[dict], plugins: list[dict]) -> list[dict]:
    """
    Merge plugins from two sources, giving priority to priority_plugins.

    Parameters:
        priority_plugins: the list of plugins from a higher-priority source.
        plugins: the list of plugins from a lower-priority source.

    Returns:
        Merged list of plugins with priority given to priority_plugins.
    """
    merged_plugins = priority_plugins.copy()

    # names are unique, we can't have 2 and more different plugins with one name
    priority_plugins_names = [p["name"] for p in priority_plugins]

    for plugin in plugins:
        if plugin["name"] not in priority_plugins_names:
            merged_plugins.append(plugin)

    return merged_plugins
