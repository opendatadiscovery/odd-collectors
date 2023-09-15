from typing import Any, NamedTuple


class ColumnMetadata(NamedTuple):
    table_catalog: Any
    table_schema: Any
    table_name: Any
    column_name: Any
    ordinal_position: Any
    column_default: Any
    is_nullable: Any
    data_type: Any
    character_maximum_length: Any
    character_octet_length: Any
    numeric_precision: Any
    numeric_scale: Any
    datetime_precision: Any
    character_set_name: Any
    collation_name: Any
    column_key: Any
