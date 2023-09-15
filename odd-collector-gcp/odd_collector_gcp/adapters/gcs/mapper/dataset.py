import re
from typing import Any, Dict
from funcy import lflatten
from lark import Lark
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
    DataSetField,
    Type,
)
from oddrn_generator.generators import GCSGenerator
from oddrn_generator.utils import escape
from pyarrow import Schema

from .column import map_column
from ..logger import logger

from .gcs_field_type_transformer import GCSFieldTypeTransformer

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/gcs.json"
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
field_type_transformer = GCSFieldTypeTransformer()
parser = Lark.open(
    "grammar/gcs_field_type_grammar.lark", rel_to=__file__, parser="lalr", start="type"
)


def __parse(field_type: str) -> Dict[str, Any]:
    try:
        column_tree = parser.parse(field_type)
        return field_type_transformer.transform(column_tree)
    except Exception as exc:
        logger.warning(f"Could not map field type: {field_type}")
        logger.debug(exc, exc_info=True)
        return {"type": "unknown", "logical_type": field_type}


def gcs_path_to_name(path: str) -> str:
    """
    Remove the bucket name from the path and return the name of the file.
    """

    return escape(re.sub("^[a-zA-z-.\d]*\/", "", path.strip("/"), 1))


def map_dataset(
    gcs_dataset: Any,
    oddrn_gen: GCSGenerator,
) -> DataEntity:
    name = gcs_path_to_name(gcs_dataset.prefix)

    oddrn_gen.set_oddrn_paths(keys=name)
    metadata = [
        {
            "schema_url": f"{SCHEMA_FILE_URL}#/definitions/gcsDataSetExtension",
            "metadata": gcs_dataset.metadata,
        }
    ]

    columns = map_columns(gcs_dataset.schema, oddrn_gen)

    return DataEntity(
        name=name,
        oddrn=oddrn_gen.get_oddrn_by_path("keys", name),
        metadata=metadata,
        # TODO for updated_at and Created_at we need first and last files mod date from GCS, arrow file info shows only size
        updated_at=None,
        created_at=None,
        type=DataEntityType.FILE,
        dataset=DataSet(rows_number=gcs_dataset.rows_number, field_list=columns),
    )


def map_columns(schema: Schema, oddrn_gen: GCSGenerator) -> list[DataSetField]:
    columns = [
        map_column(oddrn_gen, __parse(str(field.type)), field.name) for field in schema
    ]

    return lflatten(columns)
