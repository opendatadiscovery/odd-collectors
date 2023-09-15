from typing import Dict, Type

from odd_collector_sdk.domain.plugin import Plugin

PluginFactory = Dict[str, Type[Plugin]]
