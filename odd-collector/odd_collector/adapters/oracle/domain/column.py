from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

import sqlalchemy.sql.sqltypes as sqltype
from funcy import omit
from odd_collector.helpers.bytes_to_str import convert_bytes_to_str_in_dict


@dataclass
class Column:
    name: str
    type: sqltype
    is_literal: Optional[bool]
    is_primary_key: Optional[bool]
    is_foreign_key: Optional[bool]
    is_nullable: Optional[bool]
    default: Optional[Any]
    index: Optional[Any]
    unique: Optional[Any]
    comment: Optional[str]
    logical_type: Optional[str]

    @property
    def metadata(self):
        return omit(self, ["name", "type"])

    @property
    def odd_metadata(self) -> Dict[str, str]:
        return convert_bytes_to_str_in_dict(asdict(self))
