from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import AzureSQLGenerator


def map_database(
    generator: AzureSQLGenerator, service_name: str, entities: List[str]
) -> DataEntity:
    """
    :param generator - generator
    :param service_name - str
    :param entities - list of Table | View oddrn
    """
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=service_name,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
