from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import OracleGenerator

from ..domain import Table
from .column import map_column
from .metadata import dataset_metadata


def map_table(generator: OracleGenerator, table: Table) -> DataEntity:
    generator.set_oddrn_paths(tables=table.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        description=table.description,
        metadata=[dataset_metadata(entity=table)],
        dataset=DataSet(
            field_list=[
                map_column(generator, "tables_columns", column)
                for column in table.columns
            ]
        ),
    )
