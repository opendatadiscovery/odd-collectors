import datetime
from dataclasses import dataclass, field
from typing import Any, Optional, Union


@dataclass
class File:
    path: str
    base_name: str
    schema: Any
    format: str
    mtime: Optional[datetime.datetime] = None
    metadata: Optional[dict] = field(default_factory=dict)

    @classmethod
    def dataset(
        cls, path: str, name: str, schema: Any, file_format: str, metadata: dict = None
    ):
        return cls(
            path=path,
            base_name=name,
            schema=schema,
            format=file_format,
            metadata=metadata,
        )

    @classmethod
    def unknown(cls, path: str, base_name: str, file_format: str):
        return cls(path=path, base_name=base_name, schema=None, format=file_format)


@dataclass
class Folder:
    path: str
    objects: list[Union["Folder", File]] = field(default_factory=list)


@dataclass
class Bucket:
    name: str
    objects: list[Union[Folder, File]] = field(default_factory=list)
