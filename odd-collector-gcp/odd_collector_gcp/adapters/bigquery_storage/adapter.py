from google.cloud import bigquery
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import BigQueryDataset
from odd_collector_gcp.adapters.bigquery_storage.mapper import BigQueryStorageMapper
from odd_collector_gcp.domain.plugin import BigQueryStoragePlugin


class Adapter(BaseAdapter):
    config: BigQueryStoragePlugin
    generator: BigQueryStorageGenerator

    def __init__(self, config: BigQueryStoragePlugin):
        super().__init__(config)
        self.client = bigquery.Client(project=config.project)
        self.mapper = BigQueryStorageMapper(oddrn_generator=self.generator)

    def create_generator(self) -> BigQueryStorageGenerator:
        return BigQueryStorageGenerator(
            google_cloud_settings={"project": self.config.project},
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = self.mapper.map_datasets(self.__fetch_datasets())
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )

    def __fetch_datasets(self) -> list[BigQueryDataset]:
        datasets = []
        datasets_iterator = self.client.list_datasets(page_size=self.config.page_size)
        for datasets_page in datasets_iterator.pages:
            for dr in datasets_page:
                if self.config.datasets_filter.is_allowed(dr.dataset_id):
                    tables_iterator = self.client.list_tables(
                        dr, page_size=self.config.page_size
                    )
                    dataset = BigQueryDataset(
                        data_object=self.client.get_dataset(dr.dataset_id),
                        tables=[
                            self.client.get_table(t)
                            for tables_page in tables_iterator.pages
                            for t in tables_page
                        ],
                    )
                    datasets.append(dataset)

        return datasets
