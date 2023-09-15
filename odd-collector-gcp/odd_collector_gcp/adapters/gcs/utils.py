import re
from typing import Union

from pyarrow._csv import ParseOptions
from pyarrow._dataset import CsvFileFormat

from odd_collector_gcp.adapters.gcs.logger import logger


def file_extension(file_name: str) -> str:
    """
    Get file extension from path.
    @param file_name: path to file
    @return: file extension, e.g. csv, parquet, etc.
    """
    try:
        return re.search("\.(?P<ext>[a-z]+)", file_name).group("ext")
    except AttributeError:
        logger.error(f"Failed to get file extension for {file_name}")
        raise


def file_format(file_name: str) -> Union[str, CsvFileFormat]:
    """
    Get file format from path.
    @param file_name: path to file
    @return: str or CsvFileFormat
    """
    extension = file_extension(file_name)

    if extension in ["csv", "csv.gz", "csv.bz2"]:
        return "csv"
    elif extension in ["tsv", "tsv.gz", "tsv.bz2"]:
        return CsvFileFormat(ParseOptions(delimiter="\t"))
    elif extension in ["parquet"]:
        return "parquet"
    else:
        logger.warning(f"No available parser for {extension=}")
        return extension
