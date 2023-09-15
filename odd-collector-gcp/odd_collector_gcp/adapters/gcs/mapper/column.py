from typing import Any

from funcy import lflatten
from lark import Lark
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import GCSGenerator
from pyarrow import Schema

from odd_collector_gcp.adapters.gcs.mapper.gcs_field_type_transformer import (
    field_type_transformer,
)

from ..logger import logger

TYPE_MAP: dict[str, Type] = {
    "int8": Type.TYPE_INTEGER,
    "int16": Type.TYPE_INTEGER,
    "int32": Type.TYPE_INTEGER,
    "int64": Type.TYPE_INTEGER,
    "uint8": Type.TYPE_INTEGER,
    "uint16": Type.TYPE_INTEGER,
    "uint32": Type.TYPE_INTEGER,
    "uint64": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "float8": Type.TYPE_NUMBER,
    "float16": Type.TYPE_NUMBER,
    "float32": Type.TYPE_NUMBER,
    "float64": Type.TYPE_NUMBER,
    "time32": Type.TYPE_TIME,
    "time64": Type.TYPE_TIME,
    "timestamp": Type.TYPE_DATETIME,
    "date32": Type.TYPE_DATETIME,
    "date32[day]": Type.TYPE_DATETIME,
    "date64": Type.TYPE_DATETIME,
    "duration": Type.TYPE_DURATION,
    "month_day_nano_interval": Type.TYPE_DURATION,
    "binary": Type.TYPE_BINARY,
    "string": Type.TYPE_STRING,
    "utf8": Type.TYPE_STRING,
    "large_binary": Type.TYPE_BINARY,
    "large_string": Type.TYPE_STRING,
    "large_utf8": Type.TYPE_STRING,
    "decimal128": Type.TYPE_NUMBER,
    "list": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "struct": Type.TYPE_STRUCT,
    "union": Type.TYPE_UNION,
    "double": Type.TYPE_NUMBER,
    "bool": Type.TYPE_BOOLEAN,
    "dictionary": Type.TYPE_STRUCT,
    "unknown": Type.TYPE_UNKNOWN,
}

parser = Lark.open(
    "grammar/gcs_field_type_grammar.lark", rel_to=__file__, parser="lalr", start="type"
)


def parse(field_type: str) -> dict[str, Any]:
    try:
        column_tree = parser.parse(field_type)
        return field_type_transformer.transform(column_tree)
    except Exception as exc:
        logger.warning(f"Could not map field type: {field_type}. {exc}")
        return {"type": "unknown", "logical_type": field_type}


def map_column(
    generator: GCSGenerator,
    type_parsed: dict[str, Any],
    column_name: str = None,
    parent_oddrn: str = None,
    column_description: str = None,
    stats: DataSetField = None,
    is_key: bool = None,
    is_value: bool = None,
) -> list[DataSetField]:
    result: list = []

    ds_type = type_parsed["type"]
    logical_type = str(type_parsed.get("logical_type", ds_type))
    name = (
        column_name
        if column_name is not None
        else type_parsed.get("field_name", ds_type)
    )

    resource_name = "keys" if is_key else "values" if is_value else "subcolumns"

    dsf = DataSetField(
        name=name,
        oddrn=(
            generator.get_oddrn_by_path("columns", name)
            if parent_oddrn is None
            else f"{parent_oddrn}/{resource_name}/{name}"
        ),
        parent_field_oddrn=parent_oddrn,
        type=DataSetFieldType(
            type=TYPE_MAP.get(
                ds_type, Type.TYPE_UNKNOWN
            ),  # TYPE_MAP.get(str(field.type), TYPE_MAP.get(type(field.type), Type.TYPE_UNKNOWN)),
            logical_type=logical_type,
            is_nullable=True,
        ),
        is_key=is_key,
        is_value=is_value,
        owner=None,
        metadata=[],
        stats=stats or None,
        default_value=None,
        description=column_description,
    )
    result.append(dsf)

    if ds_type in {"struct", "union"}:
        for children in type_parsed["children"]:
            result.extend(
                map_column(
                    generator=generator, parent_oddrn=dsf.oddrn, type_parsed=children
                )
            )

    if ds_type == "list":
        for children in type_parsed["children"]:
            result.extend(
                map_column(
                    generator=generator,
                    parent_oddrn=dsf.oddrn,
                    type_parsed=children,
                    is_value=True,
                )
            )

    if ds_type == "map":
        result.extend(
            map_column(
                generator=generator,
                parent_oddrn=dsf.oddrn,
                type_parsed=type_parsed["key_type"],
                is_key=True,
            )
        )

        result.extend(
            map_column(
                generator=generator,
                parent_oddrn=dsf.oddrn,
                type_parsed=type_parsed["value_type"],
                is_value=True,
            )
        )

    return result


def map_columns(schema: Schema, generator: GCSGenerator) -> list[DataSetField]:
    columns = [
        map_column(generator, parse(str(field.type)), field.name) for field in schema
    ]

    return lflatten(columns)
