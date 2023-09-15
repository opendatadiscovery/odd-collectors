from typing import Dict, Union

import pyarrow.dataset as ds
from oddrn_generator.generators import GCSGenerator
from pyarrow._csv import ParseOptions
from pyarrow._dataset import CsvFileFormat, FileFormat

from odd_collector_gcp.adapters.gcs.mapper.dataset import map_dataset
from odd_collector_gcp.domain.to_data_entity import ToDataEntity
from odd_collector_gcp.errors import InvalidFileFormatWarning
from odd_collector_gcp.utils.parse_gcs_url import parse_gcs_url


def get_dataset_class(file_path: str):
    for subclass in GCSDataset.__subclasses__():
        if file_path.endswith(subclass.supported_formats):
            return subclass
    raise InvalidFileFormatWarning(
        f"Got {file_path}, available formats are {AVAILABLE_FILE_FORMATS}"
    )


class GCSDataset(ToDataEntity):
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
        if self._path.startswith("gs://"):
            bucket, _ = parse_gcs_url(self._path)
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

    def to_data_entity(self, oddrn_generator: GCSGenerator):
        return map_dataset(self, oddrn_generator)

    @classmethod
    def get_format(cls) -> Union[str, "FileFormat"]:
        return cls.format


class CSVGCSDataset(GCSDataset):
    format = "csv"
    supported_formats = (".csv", ".csv.gz", ".csv.bz2")


class TSVGCSDataset(GCSDataset):
    format = "tsv"
    supported_formats = (".tsv", ".tsv.gz", ".tsv.bz2")

    @classmethod
    def get_format(cls) -> "CsvFileFormat":
        """
        Using csv format with 'tab' delimiter for tsv files
        """
        return CsvFileFormat(ParseOptions(delimiter="\t"))


class ParquetGCSDataset(GCSDataset):
    format = "parquet"
    supported_formats = (".parquet",)


AVAILABLE_FILE_FORMATS = ", ".join(
    [", ".join(subclass.supported_formats) for subclass in GCSDataset.__subclasses__()]
)
