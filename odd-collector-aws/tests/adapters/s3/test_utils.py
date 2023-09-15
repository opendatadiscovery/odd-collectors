import pytest
from pyarrow._csv import ParseOptions
from pyarrow._dataset import CsvFileFormat

from odd_collector_aws.adapters.s3.utils import file_extension, file_format


def test_file_extension():
    assert file_extension("s3://bucket/folder/file.csv") == "csv"
    assert file_extension("s3://bucket/folder/file.csv.tar") == "csv"

    with pytest.raises(AttributeError):
        file_extension("s3://bucket/folder/file")


def test_file_format():
    assert file_format("s3://bucket/folder/file.csv") == "csv"
    assert file_format("s3://bucket/folder/file.csv.tar") == "csv"

    assert file_format("s3://bucket/folder/file.tsv.tar") == CsvFileFormat(
        ParseOptions(delimiter="\t")
    )
