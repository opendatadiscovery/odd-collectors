from odd_collector_sdk.logger import logger


def get_version() -> str:
    try:
        from odd_collector_sdk.__version__ import VERSION

        return VERSION
    except ImportError as e:
        logger.debug(f"Can't get version from odd_collector_sdk.__version__. {e}")
        return "-"
