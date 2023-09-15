import sys
from os import path

from odd_collector_sdk.domain.collector_config import load_config
from odd_collector_sdk.load_adapter import load_adapters
from tests.plugins.plugins import PLUGIN_FACTORY

test_folder_path = path.realpath(path.dirname(__file__))


def test_importing_modules():
    config_path = path.join(test_folder_path, "collector_config.yaml")
    config = load_config(config_path, PLUGIN_FACTORY)

    package_name = "tests.adapters.glue"

    assert package_name not in sys.modules
    assert f"{package_name}.adapter" not in sys.modules

    adapters = load_adapters("tests.adapters", config.plugins)
    assert f"{package_name}.sub_pkg" in sys.modules
    assert f"{package_name}.sub_pkg.sub_module" in sys.modules

    assert len(adapters) == 2
    assert package_name in sys.modules
    assert f"{package_name}.adapter" in sys.modules

    for adapter in adapters:
        assert hasattr(adapter, "config")
