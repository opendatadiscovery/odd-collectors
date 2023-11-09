from funcy import filter, lfilter
from odd_models import DataEntity
from odd_models.models import DataEntityList, DataEntityType, DataSetField


def find_by_type(
    data_entity_list: DataEntityList, data_entity_type: DataEntityType
) -> list[DataEntity]:
    """Find data entities by type."""
    return lfilter(
        lambda data_entity: data_entity.type == data_entity_type, data_entity_list.items
    )


def find_by_name(data_entity_list: DataEntityList, name: str) -> DataEntity:
    return next(
        filter(
            lambda data_entity: data_entity.name.lower() == name.lower(),
            data_entity_list.items,
        )
    )


def find_dataset_field_by_name(dataset_field_list: list[DataSetField], name: str) -> DataSetField:
    return next(
        filter(
            lambda dataset_field: dataset_field.name.lower() == name.lower(),
            dataset_field_list,
        )
    )
