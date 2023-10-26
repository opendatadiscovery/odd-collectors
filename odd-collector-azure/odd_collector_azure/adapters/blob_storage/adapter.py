from typing import Iterable, Union

from odd_collector_azure.adapters.blob_storage.file_system import FileSystem
from odd_collector_azure.adapters.blob_storage.mapper.container import map_container
from odd_collector_azure.domain.plugin import BlobPlugin
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import AzureBlobStorageGenerator
from oddrn_generator.generators import Generator

from .logger import logger


class Adapter(BaseAdapter):
    config: BlobPlugin
    generator: Union[Generator, AzureBlobStorageGenerator]

    def __init__(self, config: BlobPlugin):
        super().__init__(config)
        self.fs = FileSystem(config)

    def create_generator(self) -> Generator:
        return AzureBlobStorageGenerator(
            azure_cloud_settings={
                "account": self.config.account_name,
                "container": self.config.dataset_config.container,
            }
        )

    def get_data_entity_list(self) -> Iterable[DataEntityList]:
        logger.debug(
            f"Getting data entities for {self.config.dataset_config.container} container"
        )
        try:
            container = self.fs.get_container()
            data_entities = map_container(container, self.generator)

            yield DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=list(data_entities),
            )
        except Exception as e:
            logger.error(
                f"Error while processing container {self.config.dataset_config.container}: {e}."
                " SKIPPING."
            )
