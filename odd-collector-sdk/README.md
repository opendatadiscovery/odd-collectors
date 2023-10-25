[![PyPI version](https://badge.fury.io/py/odd-collector-sdk.svg)](https://badge.fury.io/py/odd-collector-sdk)

# ODD Collector SDK
Root project for ODD collectors

### Domain
* `CollectorConfig`

    _Main config file for collector_
    ``` python
    class CollectorConfig(pydantic.BaseSettings):
        default_pulling_interval: int # pulling interval in minutes
        token: str                    # token for requests to odd-platform
        plugins: Any
        platform_host_url: str
    ```

* `Collector`

    Args:

    `config_path`: str - path to collector_config.yaml (i.e. `'/collector_config.yaml'`)

    `root_package`: str - root package for adapters which will be loaded (i.e. `'my_collector.adapters'`)

    `plugins_union_type` - Type variable for pydantic model.

* `Plugin`

  Is a config for adapter
  ```python
  class Plugin(pydantic.BaseSettings):
    name: str
    description: Optional[str] = None
    namespace: Optional[str] = None
  ```

  Plugin class inherited from Pydantic's BaseSetting,it means it can take any field, which was skipped in `collector_config.yaml`, from env variables.

  Field `type: Literal["custom_adapter"]`  is obligatory for each plugin, by convention literal **MUST** have same name with adapter package

  Plugins example:
  ```python
    # plugins.py
    class AwsPlugin(Plugin):
        aws_secret_access_key: str
        aws_access_key_id: str
        aws_region: str
    
    class S3Plugin(AwsPlugin):
        type: Literal["s3"]
        buckets: Optional[List[str]] = []

    class GluePlugin(AwsPlugin):
        type: Literal["glue"]
    
    # For Collector's plugins_union_type argument
    AvailablePlugin = Annotated[
        Union[
            GluePlugin,
            S3Plugin,
        ],
        pydantic.Field(discriminator="type"),
    ]
  ```
* AbstractAdapter
    Abstract adapter which **MUST** be implemented by generic adapters

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



### Adapters folder
Each adapter inside adapters folder must have an `adapter.py` file with an `Adapter` class implementing `AbstractAdapter`
```python
    # custom_adapter/adapter.py example
    from odd_collector_sdk.domain.adapter import AbstractAdapter
    from odd_models.models import DataEntityList

    # 
    class Adapter(AbstractAdapter):
        def __init__(self, config: any) -> None:
            super().__init__()

        def get_data_entity_list(self) -> DataEntityList:
            return DataEntityList(data_source_oddrn="test")

        def get_data_source_oddrn(self) -> str:
            return "oddrn"
```

### Plugins
Each plugin must implement `Plugin` class from sdk
```python
    # domain/plugins.py
    from typing import Literal, Union
    from typing_extensions import Annotated

    import pydantic
    from odd_collector_sdk.domain.plugin import Plugin

    class CustomPlugin(Plugin):
        type: Literal["custom_adapter"]


    class OtherCustomPlugin(Plugin):
        type: Literal["other_custom_adapter"]

    # Needs this type variable for Collector initialization
    AvailablePlugins = Annotated[
        Union[CustomPlugin, OtherCustomPlugin],
        pydantic.Field(discriminator="type"),
    ]
```

### collector_config.yaml

```yaml
default_pulling_interval: 10 
token: "" 
platform_host_url: "http://localhost:8080" 
plugins:
  - type: custom_adapter
    name: custom_adapter_name
  - type: other_custom_adapter
    name: other_custom_adapter_name

```

## Usage
```python
# __main__.py

import asyncio
import logging
from os import path


from odd_collector_sdk.collector import Collector

# Union type of avalable plugins
from my_collector.domain.plugins import AvailablePlugins

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)

try:
    cur_dirname = path.dirname(path.realpath(__file__))
    config_path = path.join(cur_dirname, "../collector_config.yaml")
    root_package = "my_collector.adapters"

    loop = asyncio.get_event_loop()

    collector = Collector(config_path, root_package, AvailablePlugin)

    loop.run_until_complete(collector.register_data_sources())

    collector.start_polling()
    loop.run_forever()
except Exception as e:
    logging.error(e, exc_info=True)
    loop.stop()
```

And run
```bash
poetry run python -m my_collector
```


