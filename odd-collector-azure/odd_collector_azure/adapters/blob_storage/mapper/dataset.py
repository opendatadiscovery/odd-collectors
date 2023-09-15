import logging
import re
from typing import Any, Dict, List

from funcy import lflatten
from lark import Lark
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
    DataSetField,
    DataSetFieldType,
    Type,
)
from oddrn_generator.utils import escape
from pyarrow import Schema

from odd_collector_azure.adapters.blob_storage.blob_generator import BlobGenerator
from .azure_file_type_transformer import AzureFieldTypeTransformer

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/s3.json"
)

# TODO: create common grammar rule for date
TYPE_MAP: Dict[str, Type] = {
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
field_type_transformer = AzureFieldTypeTransformer()
parser = Lark.open(
    "grammar/blob_storage_field_type_grammar.lark", rel_to=__file__, parser="lalr", start="type"
)


def __parse(field_type: str) -> Dict[str, Any]:
    try:
        column_tree = parser.parse(field_type)
        return field_type_transformer.transform(column_tree)
    except Exception as exc:
        logging.warning(f"Could not map field type: {field_type}")
        logging.debug(exc, exc_info=True)
        return {"type": "unknown", "logical_type": field_type}


def s3_path_to_name(path: str) -> str:
    """
    Remove the container name from the path and return the name of the file.
    """

    return escape(re.sub("^[a-zA-z-.\d]*\/", "", path.strip("/"), 1))


def map_dataset(
    s3_dataset: Any,
    oddrn_gen: BlobGenerator,
) -> DataEntity:
    name = s3_path_to_name(s3_dataset.prefix)

    oddrn_gen.set_oddrn_paths(keys=name)
    metadata = [
        {
            "schema_url": f"{SCHEMA_FILE_URL}#/definitions/AzureBlobStorageDataSetExtension",
            "metadata": s3_dataset.metadata,
        }
    ]

    columns = map_columns(s3_dataset.schema, oddrn_gen)

    return DataEntity(
        name=name,
        oddrn=oddrn_gen.get_oddrn_by_path("keys", name),
        metadata=metadata,
        updated_at=None,
        created_at=None,
        type=DataEntityType.FILE,
        dataset=DataSet(rows_number=s3_dataset.rows_number, field_list=columns),
    )


def map_column(
    oddrn_gen: BlobGenerator,
    type_parsed: Dict[str, Any],
    column_name: str = None,
    parent_oddrn: str = None,
    column_description: str = None,
    stats: DataSetField = None,
    is_key: bool = None,
    is_value: bool = None,
) -> List[DataSetField]:
    result: list = []
    ds_type = type_parsed["type"]
    logical_type = str(type_parsed.get("logical_type", ds_type))
    name = (
        column_name
        if column_name is not None
        else type_parsed["field_name"]
        if "field_name" in type_parsed
        else ds_type
    )

    resource_name = "keys" if is_key else "values" if is_value else "subcolumns"

    dsf = DataSetField(
        name=name,
        oddrn=(
            oddrn_gen.get_oddrn_by_path("columns", name)
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
        is_key=bool(is_key),
        is_value=bool(is_value),
        owner=None,
        metadata=[],
        stats=stats or None,
        default_value=None,
        description=column_description,
    )
    result.append(dsf)

    if ds_type in ["struct", "union"]:
        for children in type_parsed["children"]:
            result.extend(
                map_column(
                    oddrn_gen=oddrn_gen, parent_oddrn=dsf.oddrn, type_parsed=children
                )
            )

    if ds_type == "list":
        for children in type_parsed["children"]:
            result.extend(
                map_column(
                    oddrn_gen=oddrn_gen,
                    parent_oddrn=dsf.oddrn,
                    type_parsed=children,
                    is_value=True,
                )
            )

    if ds_type == "map":
        result.extend(
            map_column(
                oddrn_gen=oddrn_gen,
                parent_oddrn=dsf.oddrn,
                type_parsed=type_parsed["key_type"],
                is_key=True,
            )
        )

        result.extend(
            map_column(
                oddrn_gen=oddrn_gen,
                parent_oddrn=dsf.oddrn,
                type_parsed=type_parsed["value_type"],
                is_value=True,
            )
        )

    return result


def map_columns(schema: Schema, oddrn_gen: BlobGenerator) -> List[DataSetField]:
    columns = [
        map_column(oddrn_gen, __parse(str(field.type)), field.name) for field in schema
    ]

    return lflatten(columns)
