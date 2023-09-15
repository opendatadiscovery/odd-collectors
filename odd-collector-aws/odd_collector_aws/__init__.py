from odd_collector_aws.logger import logger


def get_version() -> str:
    try:
        from odd_collector_aws.__version__ import VERSION

        return VERSION
    except Exception as e:
        logger.warning(f"Can't get version from odd_collector_aws.__version__. {e}")
        return "-"
