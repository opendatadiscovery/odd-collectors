from typing import Optional

from pydantic import BaseModel


class FolderAsDataset(BaseModel):
    """
    Configuration for folder as dataset.
    If folder is a dataset, then all files in the folder will be treated as a single dataset.
    """

    file_format: str
    flavor: str = None
    field_names: Optional[list[str]] = None


class DatasetConfig(BaseModel):
    bucket: str
    prefix: Optional[str]
    folder_as_dataset: Optional[FolderAsDataset] = None

    @property
    def full_path(self) -> str:
        bucket = self.bucket.strip("/")
        prefix = self.prefix

        return f"{bucket}/{prefix.strip('/')}" if prefix else bucket
