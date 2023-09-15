from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import AzureSQLGenerator

from ..domain import Table
from .columns import map_column
from .models import ColumnMetadata


def map_table(generator: AzureSQLGenerator, table: Table) -> DataEntity:
    generator.set_oddrn_paths(schemas=table.schema, tables=table.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        description=table.description,
        created_at=table.create_date,
        updated_at=table.modify_date,
        dataset=DataSet(
            rows_number=table.row_count,
            field_list=[
                map_column(generator, "tables_columns", ColumnMetadata(**column))
                for column in table.columns
            ],
        ),
    )
