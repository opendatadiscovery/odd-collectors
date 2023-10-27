from typing import Dict, List, Type

from odd_collector_azure.domain.plugin import PowerBiPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import PowerBiGenerator

from .client import PowerBiClient
from .mappers.dashboards import map_dashboard
from .mappers.datasets import map_dataset


class Adapter(AbstractAdapter):
    def __init__(
        self, config: PowerBiPlugin, client: Type[PowerBiPlugin] = None
    ) -> None:
        client = client or PowerBiClient
        self.client = client(config)

        self.__oddrn_generator = PowerBiGenerator(
            azure_cloud_settings={"domain": config.domain}
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    @staticmethod
    def __create_dataset_id_oddrn_map(
        datasets_entities: List[DataEntity],
    ) -> Dict[str, str]:
        return {
            dataset_entity.metadata[0].metadata["id"]: dataset_entity.oddrn
            for dataset_entity in datasets_entities
        }

    async def __get_datasets_entities(self) -> List[DataEntity]:
        datasets = await self.client.get_datasets()
        enriched_datasets = await self.client.enrich_datasets_with_datasources_oddrns(
            datasets
        )
        return [
            map_dataset(self.__oddrn_generator, dataset)
            for dataset in enriched_datasets
        ]

    async def get_data_entity_list(self) -> DataEntityList:
        dashboards = await self.client.get_dashboards()
        dashboard_datasets_map = await self.client.get_datasets_ids_for_dashboards(
            [dashboard.id for dashboard in dashboards]
        )
        datasets_entities = await self.__get_datasets_entities()
        datasets_oddrns_map = self.__create_dataset_id_oddrn_map(datasets_entities)
        dashboards_entities = [
            map_dashboard(
                self.__oddrn_generator,
                dashboard,
                dashboard_datasets_map[dashboard.id],
                datasets_oddrns_map,
            )
            for dashboard in dashboards
        ]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*datasets_entities, *dashboards_entities],
        )
