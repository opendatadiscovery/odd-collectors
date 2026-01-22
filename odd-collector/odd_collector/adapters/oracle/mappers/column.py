from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import OracleGenerator

from ..domain import Column
from .column_type import map_type
from .metadata import column_metadata


def map_column(
    generator: OracleGenerator, column_path: str, column: Column
) -> DataSetField:
    """
    Maps column to DataSetField
    :param generator - Oddrn generator
    :param column_path - parent type 'tables_column' | 'views_column'
    :param column - Column model
    """
    generator.set_oddrn_paths(**{column_path: column.name})
    return DataSetField(
        name=column.name,
        oddrn=generator.get_oddrn_by_path(column_path),
        is_primary_key=column.is_primary_key,
        metadata=[column_metadata(entity=column)],
        type=DataSetFieldType(
            type=map_type(column.type),
            is_nullable=column.is_nullable,
            logical_type=str(column.logical_type),
        ),
    )
