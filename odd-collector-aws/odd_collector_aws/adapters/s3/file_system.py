from typing import Union

from odd_collector_aws.domain.plugin import S3Plugin

from ...domain.dataset_config import DatasetConfig
from ...filesystem.pyarrow_fs import FileSystem as PyarrowFs
from ...utils.remove_s3_protocol import remove_protocol
from .domain.models import Bucket, File, Folder
from .logger import logger
from .utils import file_format


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, config: S3Plugin):
        self.fs = PyarrowFs(config)
        self.filename_filter = config.filename_filter

    def get_folder_as_file(self, dataset_config: DatasetConfig) -> File:
        """
        Get folder as Dataset.
        @param dataset_config:
        @return: File
        """
        logger.debug(f"Getting folder dataset for {dataset_config=}")

        dataset = self.fs.get_dataset(
            path=dataset_config.full_path,
            format=dataset_config.folder_as_dataset.file_format,
            partitioning_flavor=dataset_config.folder_as_dataset.flavor,
            field_names=dataset_config.folder_as_dataset.field_names,
        )

        return File(
            path=dataset_config.full_path,
            base_name=dataset_config.full_path,
            schema=dataset.schema,
            metadata={
                "Format": dataset_config.folder_as_dataset.file_format,
                "Partitioning": dataset_config.folder_as_dataset.flavor,
                "Flavor": dataset_config.folder_as_dataset.flavor,
                "FieldNames": dataset_config.folder_as_dataset.field_names,
            },
            format=dataset_config.folder_as_dataset.file_format,
        )

    def get_bucket(self, dataset_config: DatasetConfig) -> Bucket:
        """
        Get bucket with all related objects.
        @param dataset_config:
        @return: Bucket
        """
        bucket = Bucket(dataset_config.bucket)
        if dataset_config.folder_as_dataset:
            bucket.objects.append(self.get_folder_as_file(dataset_config))
        else:
            objects = self.list_objects(path=dataset_config.full_path)
            bucket.objects.extend(objects)

        return bucket

    def list_objects(self, path: str) -> list[Union[File, Folder]]:
        """
        Recursively get objects for path.
        @param path: s3 path
        @return: list of either File or Folder
        """
        logger.debug(f"Getting objects for {path=}")
        objects = []

        for obj in self.fs.get_file_info(path):
            if obj.is_file:
                if not self.filename_filter.is_allowed(obj.base_name):
                    continue

                objects.append(self.get_file(obj.path, obj.base_name))
            else:
                objects.append(self.get_folder(obj.path))

        return objects

    def get_file(self, path: str, file_name: str = None) -> File:
        """
        Get File with schema and metadata.
        @param path: s3 path to file
        @param file_name: file name
        @return: File
        """
        path = remove_protocol(path)
        if not file_name:
            file_name = path.split("/")[-1]

        try:
            file_fmt = file_format(file_name)
            dataset = self.fs.get_dataset(path, file_fmt)

            return File.dataset(
                path=path,
                name=file_name,
                schema=dataset.schema,
                file_format=file_fmt,
                metadata={},
            )
        except Exception as e:
            logger.warning(f"Failed to get schema for file {path}: {e}")
            return File.unknown(
                path=path,
                base_name=file_name,
                file_format="unknown",
            )

    def get_folder(self, path: str, recursive: bool = True) -> Folder:
        """
        Get Folder with objects recursively.
        @param path: s3 path to
        @param recursive: Flag to recursively search nested objects
        @return: Folder class with objects and path
        """
        path = remove_protocol(path)
        objects = self.list_objects(path) if recursive else []
        return Folder(path, objects)
