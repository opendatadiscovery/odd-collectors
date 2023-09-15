from odd_models.models import DataSetField, DataSetFieldType, MetadataExtension, Type
from oddrn_generator import AzureSQLGenerator

from odd_collector_azure.helpers.bytes_to_str import convert_bytes_to_str

from .metadata import _data_set_field_metadata_schema_url
from .models import ColumnMetadata
from .types import TYPES_SQL_TO_ODD


def map_column(
    generator: AzureSQLGenerator, column_path: str, column: ColumnMetadata
) -> DataSetField:
    """
    Maps column to DataSetField
    :param generator - Oddrn generator
    :param column_path - parent type 'tables_column' | 'views_column'
    :param column - Column model
    """
    generator.set_oddrn_paths(**{column_path: column.column_name})
    mt = MetadataExtension(
        schema_url=_data_set_field_metadata_schema_url, metadata=column._asdict()
    )

    dsf = DataSetField(
        name=column.column_name,
        oddrn=generator.get_oddrn_by_path(column_path),
        metadada=[],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(
                convert_bytes_to_str(column.data_type), Type.TYPE_UNKNOWN
            ),
            logical_type=convert_bytes_to_str(column.data_type),
            is_nullable=column.is_nullable == "YES",
        ),
        default_value=convert_bytes_to_str(column.column_default),
        is_primary_key=bool(column.column_key),
    )

    dsf.metadata = [mt]

    return dsf
