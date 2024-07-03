from typing import Union

from funcy import lmap, partial
from odd_collector_aws.domain.plugin import S3DeltaPlugin
from odd_collector_aws.utils.create_generator import create_generator
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import Generator, S3Generator

from .client import DeltaClient
from .logger import logger
from .mappers.delta_table import map_delta_table

# TODO: Add tags


class Adapter(BaseAdapter):
    config: S3DeltaPlugin
    generator: Union[Generator, S3Generator]

    def __init__(self, config: S3DeltaPlugin) -> None:
        super().__init__(config)
        self.client = DeltaClient(config=config)

    def create_generator(self) -> Generator:
        return create_generator(S3Generator, self.config)

    def get_data_entity_list(self) -> DataEntityList:
        logger.debug(f"Getting data entity list for {self.config.delta_tables}")

        tables = self.client.get_table(self.config.delta_tables)
        data_entities = lmap(partial(map_delta_table, self.generator), tables)

        return DataEntityList(
            data_source_oddrn=self.generator.get_data_source_oddrn(),
            items=data_entities,
        )
