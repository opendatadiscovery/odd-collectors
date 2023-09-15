from pathlib import Path
from odd_collector_sdk.grammar_parser.build_dataset_field import DatasetFieldBuilder
from odd_models.models import DataSetField, Type
from oddrn_generator import GCSGenerator
from ..models.field import DField

DELTA_TO_ODD_TYPE_MAP: dict[str, Type] = {
    "float": Type.TYPE_NUMBER,
    "struct": Type.TYPE_STRUCT,
    "bigint": Type.TYPE_INTEGER,
    "binary": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "date": Type.TYPE_DATETIME,
    "decimal": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "void": Type.TYPE_UNKNOWN,
    "smallint": Type.TYPE_INTEGER,
    "timestamp": Type.TYPE_TIME,
    "tinyint": Type.TYPE_INTEGER,
    "array": Type.TYPE_LIST,
    "map": Type.TYPE_MAP,
    "int": Type.TYPE_INTEGER,
    "long": Type.TYPE_INTEGER,
    "string": Type.TYPE_STRING,
    "interval": Type.TYPE_DURATION,
}


def map_field(oddrn_generator: GCSGenerator, column: DField) -> list[DataSetField]:
    field_builder = DatasetFieldBuilder(
        data_source="gcs_delta",
        oddrn_generator=oddrn_generator,
        parser_config_path=Path(
            "odd_collector_gcp/adapters/gcs_delta/mappers/grammar/field_types.lark"
        ).absolute(),
        odd_types_map=DELTA_TO_ODD_TYPE_MAP,
    )
    return field_builder.build_dataset_field(column)
