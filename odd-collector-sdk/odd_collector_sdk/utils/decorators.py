from funcy import decorator
from odd_models.models import DataEntity, DataEntityList

from ..logger import logger


@decorator
def log_data_entity(call):
    data_entity: DataEntity = call()
    logger.debug(data_entity.json(exclude_none=True))
    return data_entity


@decorator
def write_data_entity_list(call, fname: str):
    data_entities: DataEntityList = call()
    with open(fname, "w+") as f:
        f.write(data_entities.json(exclude_none=True))
    return data_entities
