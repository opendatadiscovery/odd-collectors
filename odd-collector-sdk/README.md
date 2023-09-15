# ODD Collector SDK
Development kit for ODD Collectors. It describes schemas for adapters and plugins and provides base classes for adapters.
Each collector must have a config file, must be named as `collector_config.yaml`, a root package and a plugin factory.

## Collector example

### Requirenments
Use the package manager [poetry](https://python-poetry.org/) to install add odd-collector-sdk and asyncio.
```bash
poetry add odd-collector-sdk
```

### A typical top-level collector's directory layout (as an example we took poetry project)

    .
    ├── my_collector            
    │   ├── adapters            # Adapters
    │   │   ├── custom_adapter  # Some adapter package
    │   │   │   ├── adapter.py  # Entry file for adapter
    │   │   │   └── __init__.py
    │   │   ├── other_custom_adapter
    │   │   ├── ...             # Other adapters
    │   │   └── __init__.py
    │   ├── domain              # Domain models
    │   │   ├── ...
    │   │   ├── plugins.py      # Models for available plugins
    │   │   └── __init__.py
    │   ├── __init__.py         
    │   └── __main__.py         # Entry file for collector
    ├── ...
    ├── collector_config.yaml
    ├── pyproject.toml
    ├── LICENSE
    └── README.md



### Adapters folder example
Each adapter inside adapters folder must have an `adapter.py` file with an `Adapter` class derived from `BaseAdapter`.

```python
    # custom_adapter/adapter.py example
    from odd_collector_sdk.domain.adapter import BaseAdapter
    from odd_models.models import DataEntityList
    from oddrn_generator import Generator
    from my_collector.domain.plugins import CustomPlugin

    class Adapter(BaseAdapter):
        config: CustomPlugin
        generator: Generator
        
        def create_generator(self) -> Generator:
          return Generator()
        
        def get_data_entity_list(self) -> DataEntityList:
            # Some logic to create DataEntityList
            return DataEntityList()
```

### Plugins example
Each plugin is a config file for data source, it can have as parametres to connect to a datasource as additional params as filters. 
It must be derived `Plugin` class.

```python
# domain/plugins.py
from typing import Literal
from odd_collector_sdk.domain.plugin import Plugin

class CustomPlugin(Plugin):
    type: Literal["custom_adapter"]


class OtherCustomPlugin(Plugin):
    type: Literal["other_custom_adapter"]
    field: str

# Needs this type variable for Collector initialization
PLUGIN_FACTORY = {
    "custom_adapter": CustomPlugin,
    "other_custom_adapter": OtherCustomPlugin,
}
```

### collector_config.yaml example
```yaml
default_pulling_interval: 100 # Minutes to wait between runs of the job, if not set, job will be run only once
token: "******" # Token to access ODD Platform
platform_host_url: http://localhost:8080 # URL of ODD Platform instance, i.e. http://localhost:8080
chunk_size: 250 # Number of records to be sent in one request to the platform
connection_timeout_seconds: 300 # Seconds to wait for connection to the platform
max_instances: 1  # maximum number of concurrently running instances allowed
plugins:
  - type: custom_adapter
    name: custom_adapter_name
  - type: other_custom_adapter
    name: other_custom_adapter_name
    field: some_field_for_other_custom_adapter
```

## Usage
```python
# __main__.py
from pathlib import Path

from odd_collector_sdk.collector import Collector
from my_collector.domain.plugin import PLUGIN_FACTORY

COLLECTOR_PACKAGE = __package__
CONFIG_PATH = Path().cwd() / "collector_config.yaml"

if __name__ == "__main__":
    collector = Collector(
        config_path=CONFIG_PATH,
        root_package=COLLECTOR_PACKAGE,
        plugin_factory=PLUGIN_FACTORY,
    )
    collector.run()
```

