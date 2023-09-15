from dataclasses import dataclass
from datetime import datetime

from deltalake import Schema


@dataclass
class DTable:
    table_uri: str
    metadata: dict
    schema: Schema
    num_rows: int = None
    created_at: datetime = None
    updated_at: datetime = None

    @property
    def odd_metadata(self):
        return self.metadata
