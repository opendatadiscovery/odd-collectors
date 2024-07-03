from dataclasses import InitVar, dataclass, field
from typing import Any, Optional

from odd_collector.models import Column, Table


@dataclass(frozen=True)
class DatabricksColumn(Column):
    ...


@dataclass()
class DatabricksTable(Table):
    columns: Optional[list[DatabricksColumn]] = field(default_factory=list)
    odd_metadata_init: InitVar[Optional[dict[str, Any]]] = None

    def __post_init__(self, odd_metadata_init):
        self.metadata = odd_metadata_init
