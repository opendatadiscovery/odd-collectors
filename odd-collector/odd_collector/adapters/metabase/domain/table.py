from typing import Optional

from odd_collector.adapters.metabase.domain import Database
from pydantic import BaseModel, Field


class Table(BaseModel):
    id: int
    name: str
    schem: str = Field(..., alias="schema")
    db: Database
    entity_type: Optional[str] = None
    description: Optional[str] = None
    display_name: str
