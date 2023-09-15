from collections import deque

from odd_models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import S3Generator

from odd_collector_aws.adapters.s3.domain.models import Bucket, File, Folder

from .column import map_columns


def map_file(file: File, generator: S3Generator) -> DataEntity:
    bucket, *keys = file.path.split("/")
    generator.set_oddrn_paths(keys="/".join(keys))

    SCHEMA_FILE_URL = (
        "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
        "main/specification/extensions/s3.json"
    )
    metadata = [
        {
            "schema_url": f"{SCHEMA_FILE_URL}#/definitions/S3DataSetExtension",
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


def map_folder(folder: Folder, generator: S3Generator) -> tuple[str, deque[DataEntity]]:
    bucket, *keys = folder.path.split("/")
    generator.set_oddrn_paths(keys="/".join(keys))

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


def map_bucket(bucket: Bucket, generator: S3Generator) -> deque[DataEntity]:
    generator.set_oddrn_paths(buckets=bucket.name)

    res = deque()
    data_entity = DataEntity(
        oddrn=bucket.name,
        name=bucket.name,
        type=DataEntityType.DAG,
        data_entity_group=DataEntityGroup(entities_list=[]),
    )
    res.appendleft(data_entity)

    for obj in bucket.objects:
        if isinstance(obj, File):
            file_entity = map_file(obj, generator)
            res.appendleft(file_entity)
            data_entity.data_entity_group.entities_list.append(file_entity.oddrn)
        if isinstance(obj, Folder):
            dir_oddrn, items = map_folder(obj, generator)
            res = items + res
            data_entity.data_entity_group.entities_list.append(dir_oddrn)

    return res
