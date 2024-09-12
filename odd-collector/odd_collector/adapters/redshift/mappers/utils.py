from ..logger import logger


def log_metadata_debug(meta_debug_dict: dict) -> None:
    for key, value in meta_debug_dict.items():
        logger.debug(f"Query fetching '{key}' returned {value}")
        logger.debug(f"Items '{key}' count: {len(value)}")
