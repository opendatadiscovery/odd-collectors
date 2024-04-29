from typing import Optional

from pydantic import BaseModel


class Details(BaseModel):
    port: Optional[int] = None
    host: Optional[str] = None
    dbname: Optional[str] = None


class Database(BaseModel):
    engine: str
    id: int
    details: Details
