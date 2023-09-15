from odd_models.models import DataEntityList
from oddrn_generator import Generator

from odd_collector_sdk.domain.adapter import BaseAdapter
from tests.generator import TestGenerator

from .sub_pkg.sub_module import some_value


class Adapter(BaseAdapter):
    def __init__(self, config) -> None:
        super().__init__(config)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(data_source_oddrn="test")

    def create_generator(self) -> Generator:
        return TestGenerator(host_settings="test")
