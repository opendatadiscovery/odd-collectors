from typing import Dict, Union

import pyarrow.dataset as ds
from odd_collector_aws.adapters.s3.mapper.dataset import map_dataset
from odd_collector_aws.domain.to_data_entity import ToDataEntity
from odd_collector_aws.errors import InvalidFileFormatWarning
from odd_collector_aws.utils import parse_s3_url
from oddrn_generator.generators import S3Generator
from pyarrow._csv import ParseOptions
from pyarrow._dataset import CsvFileFormat, FileFormat


def get_dataset_class(file_path: str):
    for subclass in S3Dataset.__subclasses__():
        if file_path.endswith(subclass.supported_formats):
            return subclass
    raise InvalidFileFormatWarning(
        f"Got {file_path}, available formats are {AVAILABLE_FILE_FORMATS}"
    )


class S3Dataset(ToDataEntity):
    """
    Adapter between pyarrow.Dataset and DataEntity
    """

    format = None

    def __init__(
        self,
        dataset: ds.Dataset,
        path: str,
        metadata: Dict[str, str],
        partitioning: str = None,
    ) -> None:
        self._dataset = dataset
        self._path = path
        self._partitioning = (
            ds.partitioning(flavor="hive") if partitioning else partitioning
        )
        self._metadata = metadata or {}

    @property
    def bucket(self):
        if self._path.startswith("s3://"):
            bucket, _ = parse_s3_url(self._path)
        else:
            return self._path.split("/")[0]

    @property
    def arn(self):
        return self.path

    @property
    def schema(self):
        return self._dataset.schema

    @property
    def path(self):
        return self._path

    @property
    def rows_number(self):
        return None

    @property
    def metadata(self):
        return self._metadata

    def add_metadata(self, metadata: Dict[str, str]):
        self._metadata.update(metadata)

    def to_data_entity(self, oddrn_generator: S3Generator):
        return map_dataset(self, oddrn_generator)

    @classmethod
    def get_format(cls) -> Union[str, "FileFormat"]:
        return cls.format


class CSVS3Dataset(S3Dataset):
    format = "csv"
    supported_formats = (".csv", ".csv.gz", ".csv.bz2")


class TSVS3Dataset(S3Dataset):
    format = "tsv"
    supported_formats = (".tsv", ".tsv.gz", ".tsv.bz2")

    @classmethod
    def get_format(cls) -> "CsvFileFormat":
        """
        Using csv format with 'tab' delimiter for tsv files
        """
        return CsvFileFormat(ParseOptions(delimiter="\t"))


class ParquetS3Dataset(S3Dataset):
    format = "parquet"
    supported_formats = (".parquet",)


AVAILABLE_FILE_FORMATS = ", ".join(
    [", ".join(subclass.supported_formats) for subclass in S3Dataset.__subclasses__()]
)
