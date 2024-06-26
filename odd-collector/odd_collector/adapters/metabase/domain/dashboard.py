from datetime import datetime
from typing import Dict, List, Optional

from odd_collector.adapters.metabase.domain import Card, Creator
from pydantic import BaseModel

keys_to_exclude = {"id", "creator", "last-edit-info"}


class Dashboard(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    cards: Optional[List[Card]] = []
    creator: Creator
    collection_id: Optional[int] = None
    archived: bool
    collection_position: Optional[int] = None
    enable_embedding: bool
    show_in_getting_started: bool

    def get_owner(self) -> str:
        return self.creator.common_name

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            "description": self.description,
            "archived": self.archived,
            "collection_position": self.collection_position,
            "enable_embedding": self.enable_embedding,
            "show_in_getting_started": self.show_in_getting_started,
        }
