from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel


@dataclass(frozen=True)
class UniqueConstraint(BaseModel):
    created_on: Any
    database_name: str
    schema_name: str
    table_name: str
    column_names: list[str]
    constraint_name: str
