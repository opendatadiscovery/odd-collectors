import dataclasses
from typing import List


@dataclasses.dataclass
class BigTableColumn:
    name: str
    value: bytearray


@dataclasses.dataclass
class BigTableTable:
    table_id: str
    columns: List[BigTableColumn]


@dataclasses.dataclass
class BigTableInstance:
    instance_id: str
    tables: List[BigTableTable]
