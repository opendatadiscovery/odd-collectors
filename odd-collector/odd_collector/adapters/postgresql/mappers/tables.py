from funcy import lmap, partial, silent
from odd_collector.logger import logger
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import PostgresqlGenerator

from ..models import Table
from .columns import map_column
from .metadata import get_table_metadata
from .utils import has_vector_column
from .views import map_view


def map_table(generator: PostgresqlGenerator, table: Table):
    generator.set_oddrn_paths(
        **{"schemas": table.table_schema, "tables": table.table_name}
    )
    map_table_column = partial(map_column, generator=generator, path="tables")

    # If table contains vector column we consider it as a vector store, otherwise - an ordinary table
    data_entity_type = (
        DataEntityType.VECTOR_STORE
        if has_vector_column(table.columns)
        else DataEntityType.TABLE
    )

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=data_entity_type,
        owner=table.table_owner,
        description=table.description,
        metadata=[get_table_metadata(entity=table)],
        dataset=DataSet(
            rows_number=silent(int)(table.table_rows),
            field_list=lmap(map_table_column, table.columns),
        ),
    )


def map_tables(
    generator: PostgresqlGenerator,
    tables: list[Table],
) -> dict[str, DataEntity]:
    data_entities: dict[str, DataEntity] = {}

    for table in tables:
        logger.debug(f"Map table {table.table_name} {table.table_type}")

        if table.table_type in ("r", "p"):  # 'p' type means table is partitioned, but not a partition by itself
            entity = map_table(generator, table)
        elif table.table_type in ("v", "m"):
            entity = map_view(generator, table)
        else:
            logger.warning(
                f"Parsing only [BASE_TABLE, VIEW, MATERIALIZED_VIEW]. Got unknown {table.table_type=} {table.oid=}"
            )
            continue

        data_entities[table.as_dependency.uid] = entity

    return data_entities
