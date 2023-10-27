from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from aiohttp import ClientSession
from odd_collector_azure.azure.azure_client import AzureClient, RequestArgs
from odd_collector_azure.domain.plugin import PowerBiPlugin
from odd_models.models import DataEntity

from .domain.dashboard import Dashboard
from .domain.dataset import Dataset
from .mappers.datasources import datasources_factory, map_datasource


class PowerBiClient:
    def __init__(self, config: PowerBiPlugin):
        self.__client = AzureClient(config, "https://analysis.windows.net/powerbi/api")
        self.__base_url = "https://api.powerbi.com/v1.0/myorg/"

    async def __get_nodes(self, endpoint: str, params: Dict[str, Any] = None) -> dict:
        async with ClientSession() as session:
            response = await self.__client.fetch_async_response(
                session,
                RequestArgs(
                    method="GET",
                    url=self.__base_url + endpoint,
                    headers=await self.__client.build_headers(),
                    params=params,
                ),
            )
            return response["value"]

    async def get_datasets(self) -> List[Dataset]:
        datasets_nodes = await self.__get_nodes("datasets")
        return [
            Dataset(
                id=datasets_node.get("id"),
                name=datasets_node.get("name"),
                owner=datasets_node.get("configuredBy"),
            )
            for datasets_node in datasets_nodes
        ]

    async def get_dashboards(self) -> List[Dashboard]:
        dashboards_nodes = await self.__get_nodes("dashboards")
        return [
            Dashboard(
                id=dashboards_node.get("id"),
                display_name=dashboards_node.get("displayName"),
            )
            for dashboards_node in dashboards_nodes
        ]

    async def __get_tiles_nodes_for_dashboards(
        self, dashboards_ids: List[str]
    ) -> List[dict]:
        headers = await self.__client.build_headers()
        urls = [
            f"{self.__base_url}/dashboards/{dashboard_id}/tiles"
            for dashboard_id in dashboards_ids
        ]
        dashboards_with_tiles_nodes = await self.__client.fetch_all_async_responses(
            [RequestArgs("GET", url, None, headers) for url in urls]
        )
        return dashboards_with_tiles_nodes

    async def get_datasets_ids_for_dashboards(
        self, dashboards_ids: List[str]
    ) -> Dict[str, List[str]]:
        """

        :return: {dashboard_id: [dataset_id]}
        """
        dashboards_with_tiles_nodes = await self.__get_tiles_nodes_for_dashboards(
            dashboards_ids
        )
        dashboards_datasets: Dict[str, List[str]] = {}
        for dashboard_tiles_node in dashboards_with_tiles_nodes:
            value = dashboard_tiles_node["value"]
            embed_url = value[0]["embedUrl"]
            parsed_url = urlparse(embed_url)
            dashboard_id = parse_qs(parsed_url.query)["dashboardId"][0]
            dashboards_datasets.update(
                {dashboard_id: [tile["datasetId"] for tile in value]}
            )

        return dashboards_datasets

    async def __get_datasources_entities_for_dataset(
        self, dataset_id: str
    ) -> List[DataEntity]:
        datasources_nodes = await self.__get_nodes(f"datasets/{dataset_id}/datasources")
        datasources_entities: List[DataEntity] = []
        for datasource_node in datasources_nodes:
            datasource_type = datasource_node["datasourceType"]
            datasource_engine = datasources_factory.get(datasource_type)
            builder = datasource_engine(datasource_node)
            entity = map_datasource(builder)
            datasources_entities.append(entity)
        return datasources_entities

    async def enrich_datasets_with_datasources_oddrns(
        self, datasets: List[Dataset]
    ) -> List[Dataset]:
        enriched_datasets: List[Dataset] = []
        for dataset in datasets:
            datasources = await self.__get_datasources_entities_for_dataset(dataset.id)
            dataset.datasources = [datasource.oddrn for datasource in datasources]
            enriched_datasets.append(dataset)

        return enriched_datasets
