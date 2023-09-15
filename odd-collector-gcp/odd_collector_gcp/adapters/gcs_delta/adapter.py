from funcy import lmap, mapcat, partial
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import GCSGenerator

from odd_collector_gcp.domain.plugin import GCSDeltaPlugin

from .client import DeltaClient
from .logger import logger
from .mappers.delta_table import map_delta_table


# TODO: Add labels to the DataEntities


class Adapter(BaseAdapter):
    config: GCSDeltaPlugin
    generator: GCSGenerator

    def __init__(self, config: GCSDeltaPlugin) -> None:
        super().__init__(config)
        self.client = DeltaClient(config=config)

    def create_generator(self) -> GCSGenerator:
        return GCSGenerator(
            google_cloud_settings={"project": self.config.project},
        )

    def get_data_entity_list(self) -> DataEntityList:
        logger.debug(f"Getting data entity list for {self.config.delta_tables}")

        tables = mapcat(self.client.get_table, self.config.delta_tables)
        data_entities = lmap(partial(map_delta_table, self.generator), tables)

        return DataEntityList(
            data_source_oddrn=self.generator.get_data_source_oddrn(),
            items=data_entities,
        )
