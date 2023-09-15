import traceback
from importlib.metadata import version

from prettytable import PrettyTable

import odd_collector_sdk as sdk
from odd_collector_sdk.logger import logger


def print_versions(*pkg_names: str):
    for pkg_name in pkg_names:
        try:
            logger.debug(f"{pkg_name}: {version(pkg_name)}")
        except Exception as e:
            logger.debug(traceback.format_exc())
            logger.warning(f"Could not get version for: {pkg_name}. Reason: {e}")


def print_collector_packages_info(collector_package: str) -> None:
    """Printing collector version with ODD subpackages

    Args:
        collector_package (str): main collector package name
    """
    print_versions(collector_package, "odd_collector_sdk", "odd_models")


def print_deps(predefined_packages: list[tuple[str, str]]) -> None:
    """Printing collector version with ODD subpackages"""
    table = PrettyTable()
    table.header = ["Package", "Version"]

    if predefined_packages:
        for package in predefined_packages:
            table.add_row(package)

    table.add_row(["odd_collector_sdk", sdk.get_version()])
    print(table)
