from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class Collection(BaseModel):
    id: Union[int, str]
    name: str
    description: Optional[str] = None
    slug: Optional[str] = None
    archived: Optional[bool] = None
    cards_id: Optional[List[int]] = []
    dashboards_id: Optional[List[int]] = []
    can_write: bool

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "can_write": self.can_write,
        }
