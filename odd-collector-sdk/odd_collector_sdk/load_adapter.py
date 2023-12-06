from functools import cache
from importlib import import_module
from pathlib import Path
from types import ModuleType

from odd_collector_sdk.domain.adapter import Adapter
from odd_collector_sdk.logger import logger

from .domain.plugin import Plugin


def file_match(path: Path) -> bool:
    return path.name.endswith(".py") and not path.name.startswith("__")


def import_submodules(package: ModuleType) -> None:
    package_name = package.__name__
    package_path = Path(package.__file__).parent

    all_files = package_path.glob("*")
    python_modules = filter(file_match, all_files)

    for file in python_modules:
        module_name = f"{package_name}.{file.stem}"
        module = import_module(module_name)

        if file.is_dir():
            import_submodules(module)


@cache
def load_package(package_path: str) -> ModuleType:
    package = import_module(package_path)
    import_submodules(package)
    return package


def load_adapters(root_package: str, plugins: list[Plugin]) -> list[Adapter]:
    """Load adapters from plugins.

    Args:
        root_package (str): adapters root package, i.e "odd_collector.adapters"
        plugins (list[Plugin]): list of plugins (configurations) for adapters

    Returns:
        list[Adapter]: list of initialized adapters
    """
    adapters = []

    for plugin in plugins:
        logger.debug(f"Loading adapter for {plugin.type=} with {plugin.name=} plugin")
        package = load_package(f"{root_package}.{plugin.type}")

        adapter = package.adapter.Adapter(plugin)
        if not hasattr(adapter, "config") or adapter.config is None:
            adapter.config = plugin

        adapters.append(adapter)

    logger.success(f"Loaded {len(adapters)} adapters!")
    return adapters
