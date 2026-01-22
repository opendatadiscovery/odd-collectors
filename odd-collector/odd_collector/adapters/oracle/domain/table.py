from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from funcy import omit
from odd_collector.helpers.bytes_to_str import convert_bytes_to_str_in_dict

from .column import Column


@dataclass
class Table:
    name: str
    columns: List[Column]
    description: Optional[str]

    @property
    def metadata(self) -> Dict[str, Any]:
        return {}

    @property
    def odd_metadata(self) -> dict[str, str]:
        values = omit(asdict(self), ["columns"])
        return convert_bytes_to_str_in_dict(values)
