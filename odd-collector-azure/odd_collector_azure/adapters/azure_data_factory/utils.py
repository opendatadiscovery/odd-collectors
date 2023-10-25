import json
from datetime import datetime
from inspect import getmembers
from typing import Any

from funcy import omit


class ADFMetadataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj.__dict__.copy()


def is_property(parameter: Any) -> bool:
    return isinstance(parameter, property)


def get_properties(instance: Any, excluded_properties=None) -> dict[str, Any]:
    properties = {
        prop_name: value.fget(instance)
        for prop_name, value in getmembers(instance.__class__, is_property)
        if value.fget(instance) is not None
    }

    return omit(properties, excluded_properties) if excluded_properties else properties
