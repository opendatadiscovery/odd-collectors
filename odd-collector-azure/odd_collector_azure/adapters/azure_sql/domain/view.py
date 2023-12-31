from dataclasses import dataclass
from typing import Any, Dict, List

from sqlparse import format as sql_format

from .dependency import Dependency


@dataclass
class View:
    name: str
    columns: List[dict]
    view_definition: str
    schema: str
    description: str
    upstream: List[Dependency]
    downstream: List[Dependency]

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "view_definition": sql_format(self.view_definition),
        }
