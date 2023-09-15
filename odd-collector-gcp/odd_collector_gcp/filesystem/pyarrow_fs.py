import pyarrow.dataset as ds
from pyarrow._fs import FileInfo, FileSelector
from pyarrow.fs import GcsFileSystem
from odd_collector_gcp.adapters.gcs.domain.parameters import GCSAdapterParams
from odd_collector_gcp.domain.plugin import GCSPlugin

GCSConfig = GCSPlugin


class FileSystem:
    """
    FileSystem hides pyarrow.fs implementation details.
    """

    def __init__(self, params: GCSAdapterParams):
        self.fs = GcsFileSystem(**params.dict()) if params else GcsFileSystem()

    def get_file_info(self, path: str) -> list[FileInfo]:
        """
        Get file info from path.
        @param path: gcs path to file or folder
        @return: FileInfo
        """
        return self.fs.get_file_info(FileSelector(base_dir=path))

    def get_dataset(
        self, path: str, format: str, partitioning_flavor=None, field_names=None
    ) -> ds.Dataset:
        """
        Get dataset from file path.
        @param path: path to gcs object
        @param format: Should be one of available formats: https://arrow.apache.org/docs/python/api/dataset.html#file-format
        @return: Dataset
        """

        params = {"source": path, "format": format, "filesystem": self.fs}

        if partitioning_flavor or field_names:
            params |= {
                "partitioning": ds.partitioning(
                    flavor=partitioning_flavor, field_names=field_names
                )
            }

        return ds.dataset(**params)
