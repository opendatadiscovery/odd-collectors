from funcy import lpluck_attr
from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import RedshiftGenerator

from ..logger import logger


def map_database(
    generator: RedshiftGenerator,
    database_name: str,
    schemas_entities: list[DataEntity],
) -> DataEntity:
    logger.debug(f"Mapping database: {database_name}")
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases", database_name),
        name=database_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schemas_entities)
        ),
    )
