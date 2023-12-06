from pathlib import Path

from odd_collector import get_version
from odd_collector.domain.plugin import PLUGIN_FACTORY
from odd_collector_sdk.collector import Collector

import configparser

from odd_collector.logger import logger

COLLECTOR_PACKAGE = __package__
CONFIG_PATH = Path().cwd().joinpath("collector_config.yaml")
SECRETS_CONFIG_PATH = Path().cwd().joinpath("secrets_config.cfg")

if __name__ == "__main__":
    secrets_config = configparser.ConfigParser()
    secrets_config.read(SECRETS_CONFIG_PATH)

    logger.info(f"Starting collector. Version: {get_version()}")
    collector = Collector(
        secrets_config=secrets_config,
        collector_config_path=CONFIG_PATH,
        root_package=COLLECTOR_PACKAGE,
        plugin_factory=PLUGIN_FACTORY,
    )
    # collector.run()
