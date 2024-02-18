from typing import Any
from dataclasses import dataclass

from pydantic import BaseModel


@dataclass(frozen=True)
class ForeignKeyConstraint(BaseModel):
    created_on: Any
    constraint_name: str
    database_name: str
    schema_name: str
    table_name: str
    foreign_key: tuple[str, ...]
    referenced_constraint_name: str
    referenced_database_name: str
    referenced_schema_name: str
    referenced_table_name: str
    referenced_foreign_key: tuple[str, ...]
