from datetime import datetime
from typing import Any, Dict, List, Optional

from funcy import omit

from .column import Column
from .entity import Connectable


class Table(Connectable):
    table_catalog: str
    table_schema: str
    table_name: str
    table_owner: Optional[str] = None
    table_type: str
    is_transient: Optional[str] = None
    clustering_key: Optional[Any] = None
    row_count: Optional[int] = None
    retention_time: Optional[int] = None
    created: Optional[datetime] = None
    last_altered: Optional[datetime] = None
    table_comment: Optional[str] = None
    self_referencing_column_name: Optional[str] = None
    reference_generation: Optional[str] = None
    user_defined_type_catalog: Optional[str] = None
    user_defined_type_schema: Optional[str] = None
    user_defined_type_name: Optional[str] = None
    is_insertable_into: Optional[str] = None
    is_typed: Optional[str] = None
    columns: Optional[List[Column]] = []

    @property
    def metadata(self) -> Dict[str, Any]:
        exclude = [
            "table_catalog",
            "table_schema",
            "table_name",
            "table_type",
            "table_owner",
            "row_count",
            "comment",
            "last_altered",
            "created",
            "upstream",
            "downstream",
        ]

        return omit(self.__dict__, exclude)
