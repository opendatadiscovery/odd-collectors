import json
from asyncio import Protocol
from enum import Enum
from typing import Optional, Type

from flatdict import FlatDict
from funcy import complement, partial, select_values, walk_values
from odd_models.models import MetadataExtension

from odd_collector_sdk.logger import logger
from odd_collector_sdk.utils.json_encoder import CustomJSONEncoder

prefix = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions"


class HasMetadata(Protocol):
    odd_metadata: dict


class DefinitionType(Enum):
    DATASET = "DataSetExtension"
    DATASET_FIELD = "DataSetFieldExtension"


def extract_metadata(
    datasource: str,
    entity: HasMetadata,
    definition: DefinitionType,
    flatten: Optional[bool] = False,
    jsonify: Optional[bool] = False,
    json_encoder: Optional[Type[json.JSONEncoder]] = None,
) -> MetadataExtension:
    """
    :param datasource: name of datasource.
    :param entity: metadata entity.
    :param definition: definition type.
    :param flatten: flatten metadata object.
    :param jsonify: serialize metadata to display properly on UI.
    :param json_encoder: custom json encoder. Must be provided to serialize custom objects.
    """
    schema_url = f"{prefix}/{datasource}.json#/definitions/{definition.value}"
    try:
        if not hasattr(entity, "odd_metadata"):
            raise AttributeError(f"Entity {entity} must has ad attribute odd_metadata")

        data = entity.odd_metadata

        if not isinstance(data, dict):
            raise TypeError(f"Metadata must be a dict, got {type(data)}")

        if flatten:
            data = flat_dict(data)

        if jsonify:
            encode = partial(to_json, encoder=json_encoder)
            data = walk_values(encode, data)

        not_none = select_values(complement(is_none), data)

        return MetadataExtension(schema_url=schema_url, metadata=not_none)
    except Exception as error:
        logger.warning(f"Couldn't extract metadata, {error}")
        return MetadataExtension(schema_url=schema_url, metadata={})


def is_none(value) -> bool:
    return value is None


def to_json(value, encoder: Optional[Type[json.JSONEncoder]]) -> Optional[str]:
    try:
        return json.dumps(value, cls=encoder or CustomJSONEncoder)
    except Exception as error:
        logger.error(f"Could not jsonfy {value=}. {error}.\n SKIP.")
        return None


def flat_dict(data: dict) -> dict:
    flatten = FlatDict(data, delimiter=".")
    result = {}
    for k, v in flatten.items():
        if isinstance(v, FlatDict):
            v = {}
        result[k] = v

    return result
