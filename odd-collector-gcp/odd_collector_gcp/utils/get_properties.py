from inspect import getmembers
from typing import Any

from funcy import omit


def is_property(parameter: Any) -> bool:
    return isinstance(parameter, property)


def get_properties(instance: Any, excluded_properties=None) -> dict[str, Any]:
    properties = {}
    for prop_name, value in getmembers(instance.__class__, is_property):
        properties[prop_name] = value.fget(instance)

    return omit(properties, excluded_properties) if excluded_properties else properties
