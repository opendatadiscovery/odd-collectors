from collections import defaultdict
from copy import deepcopy

from odd_collector.adapters.snowflake.logger import logger
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import SnowflakeGenerator

from ..domain import Pipe, Table


def map_schemas(
    tables_with_entities: list[tuple[Table, DataEntity]],
    pipe_entities: list[tuple[Pipe, DataEntity]],
    generator: SnowflakeGenerator,
) -> list[DataEntity]:
    generator = deepcopy(generator)

    grouped: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))

    for table, entity in tables_with_entities:
        grouped[table.table_catalog][table.table_schema].add(entity.oddrn)

    for pipe, entity in pipe_entities:
        grouped[pipe.catalog][pipe.schema_name].add(entity.oddrn)

    entities = []
    for catalog, schemas in grouped.items():
        for schema, oddrns in schemas.items():
            logger.debug(f"Mapping schema: {schema}")

            generator.set_oddrn_paths(databases=catalog, schemas=schema)
            oddrn = generator.get_oddrn_by_path("schemas")
            entity = DataEntity(
                type=DataEntityType.DATABASE_SERVICE,
                name=schema,
                oddrn=oddrn,
                data_entity_group=DataEntityGroup(entities_list=list(oddrns)),
            )
            entities.append(entity)

    return entities
