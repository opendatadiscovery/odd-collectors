from collections import deque

from odd_collector_azure.adapters.blob_storage.domain.models import (
    Container,
    File,
    Folder,
)
from odd_models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import AzureBlobStorageGenerator

from .column import map_columns


def map_file(file: File, generator: AzureBlobStorageGenerator) -> DataEntity:
    _, keys = file.path.split("/", 1)
    generator.set_oddrn_paths(keys=keys)

    SCHEMA_FILE_URL = (
        "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
        "main/specification/extensions/azure_blob_storage.json"
    )
    metadata = [
        {
            "schema_url": f"{SCHEMA_FILE_URL}#/definitions/AzureBlobStorageDataSetExtension",
            "metadata": file.metadata,
        }
    ]
    data_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("keys"),
        name=file.base_name,
        type=DataEntityType.FILE,
        dataset=DataSet(field_list=[]),
        metadata=metadata,
    )

    if file.schema:
        data_entity.dataset = DataSet(
            field_list=map_columns(schema=file.schema, generator=generator)
        )

    return data_entity


def map_folder(
    folder: Folder, generator: AzureBlobStorageGenerator
) -> tuple[str, deque[DataEntity]]:
    _, keys = folder.path.split("/", 1)
    generator.set_oddrn_paths(keys=keys)

    res = deque()
    data_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("keys"),
        name=folder.path,
        type=DataEntityType.DAG,
        data_entity_group=DataEntityGroup(entities_list=[]),
    )

    res.appendleft(data_entity)

    for obj in folder.objects:
        if isinstance(obj, File):
            file_entity = map_file(obj, generator)
            data_entity.data_entity_group.entities_list.append(file_entity.oddrn)
            res.appendleft(file_entity)
        if isinstance(obj, Folder):
            oddrn, items = map_folder(obj, generator)
            res = items + res
            data_entity.data_entity_group.entities_list.append(oddrn)

    return data_entity.oddrn, res


def map_container(
    container: Container, generator: AzureBlobStorageGenerator
) -> deque[DataEntity]:
    res = deque()
    data_entity = DataEntity(
        oddrn=container.name,
        name=container.name,
        type=DataEntityType.DAG,
        data_entity_group=DataEntityGroup(entities_list=[]),
    )
    res.appendleft(data_entity)

    for obj in container.objects:
        if isinstance(obj, File):
            file_entity = map_file(obj, generator)
            res.appendleft(file_entity)
            data_entity.data_entity_group.entities_list.append(file_entity.oddrn)
        if isinstance(obj, Folder):
            dir_oddrn, items = map_folder(obj, generator)
            res = items + res
            data_entity.data_entity_group.entities_list.append(dir_oddrn)

    return res
