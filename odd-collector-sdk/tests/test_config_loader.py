import os
from contextlib import contextmanager
from pathlib import Path

import pytest

from odd_collector_sdk.domain.collector_config_loader import CollectorConfigLoader
from odd_collector_sdk.errors import LoadConfigError
from tests.plugins.plugins import PLUGIN_FACTORY


@contextmanager
def with_env_var(key, value):
    os.environ[key] = value
    yield
    os.unsetenv(key)


def test_config_loader():
    loader = CollectorConfigLoader(
        config_path=Path(__file__).parent / "./collector_config.yaml",
        plugin_factory=PLUGIN_FACTORY,
    )
    config = loader.load()

    assert len(config.plugins) == 2
    assert config.plugins[0].type == "glue"
    assert config.plugins[1].type == "s3"


def test_config_loader_with_empty_env_var():
    loader = CollectorConfigLoader(
        config_path=Path(__file__).parent / "./collector_config_with_env_var.yaml",
        plugin_factory=PLUGIN_FACTORY,
    )

    with pytest.raises(LoadConfigError):
        loader.load()


def test_config_with_env_var():
    with with_env_var("DB_USER", "postgres"):
        loader = CollectorConfigLoader(
            config_path=Path(__file__).parent / "./collector_config_with_env_var.yaml",
            plugin_factory=PLUGIN_FACTORY,
        )

        config = loader.load()
        assert config.plugins[0].type == "postgres"
        assert config.plugins[0].db_user == "postgres"
