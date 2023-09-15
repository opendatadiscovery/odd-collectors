from typing import Protocol

from odd_models.models import DataEntity


class ToDataEntity(Protocol):
    """
    Interface for models which can be mapped to DataEntity
    """

    def to_data_entity(self, *args, **kwargs) -> DataEntity:
        ...
