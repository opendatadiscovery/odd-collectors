from ..types import PluginFactory
from ..domain.collector_config import CollectorConfig


def generate_config(
    platform_connection_settings: dict,
    plugins: list[dict],
    plugin_factory: PluginFactory
) -> CollectorConfig:
    config_dict = {
        **platform_connection_settings,
        "plugins": [
            plugin_factory[plugin["type"]].parse_obj(plugin)
            for plugin in plugins
        ]
    }
    return CollectorConfig.parse_obj(config_dict)
