from typing import Iterable, Union

from odd_collector_aws.domain.plugin import S3Plugin
from odd_collector_aws.logger import logger
from odd_collector_aws.utils.create_generator import create_generator
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import Generator, S3Generator

from .file_system import FileSystem
from .mapper.bucket import map_bucket


class Adapter(BaseAdapter):
    config: S3Plugin
    generator: Union[Generator, S3Generator]

    def __init__(self, config: S3Plugin) -> None:
        super().__init__(config)
        self.fs = FileSystem(config)

    def create_generator(self) -> Generator:
        return create_generator(S3Generator, self.config)

    def get_data_entity_list(self) -> DataEntityList:
        logger.debug(
            f"Getting data entities for {self.config.dataset_config.bucket} bucket"
        )

        bucket = self.fs.get_bucket(self.config.dataset_config)
        data_entities = map_bucket(bucket, self.generator)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(data_entities),
        )
