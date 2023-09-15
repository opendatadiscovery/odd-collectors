from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class Table:
    name: str
    create_date: str
    modify_date: str
    row_count: int
    columns: List[dict]
    description: Optional[str]
    schema: str

    @property
    def metadata(self) -> Dict[str, Any]:
        return {}
