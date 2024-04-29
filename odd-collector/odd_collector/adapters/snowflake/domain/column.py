from typing import Any, Dict

from funcy import omit
from pydantic import BaseModel


class Column(BaseModel):
    table_catalog: Any = None
    table_schema: Any = None
    table_name: Any = None
    column_name: Any = None
    ordinal_position: Any = None
    column_default: Any = None
    is_nullable: Any = None
    data_type: Any = None
    character_maximum_length: Any = None
    character_octet_length: Any = None
    numeric_precision: Any = None
    numeric_precision_radix: Any = None
    numeric_scale: Any = None
    collation_name: Any = None
    is_identity: Any = None
    identity_generation: Any = None
    identity_start: Any = None
    identity_increment: Any = None
    identity_cycle: Any = None
    comment: Any = None
    is_primary_key: bool = False
    is_clustering_key: bool = False

    @property
    def metadata(self) -> Dict[str, Any]:
        excluded = [
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "column_default",
            "is_nullable",
            "data_type",
        ]
        return omit(self.__dict__, excluded)
