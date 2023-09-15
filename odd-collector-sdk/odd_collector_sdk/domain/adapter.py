from abc import ABC, abstractmethod
from typing import Union

from odd_models.models import DataEntityList
from oddrn_generator import Generator

from odd_collector_sdk.domain.plugin import Config


class AbstractAdapter(ABC):
    config: Config

    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        pass

    @abstractmethod
    def get_data_entity_list(self) -> DataEntityList:
        pass


class BaseAdapter(AbstractAdapter, ABC):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config
        self.generator = self.create_generator()

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    @abstractmethod
    def create_generator(self) -> Generator:
        ...


class AsyncAbstractAdapter(ABC):
    config: Config

    @abstractmethod
    def get_data_source_oddrn(self) -> str:
        pass

    @abstractmethod
    async def get_data_entity_list(self) -> DataEntityList:
        pass


Adapter = Union[BaseAdapter, AsyncAbstractAdapter, AbstractAdapter]
