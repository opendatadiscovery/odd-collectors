from typing import Any, Callable, Dict, Optional, Union

from pydantic import BaseModel, Field


class PaginatorConfig(BaseModel):
    op_name: str
    parameters: Dict[str, Union[str, int]] = Field(default_factory=dict)
    page_size: Optional[int] = None
    payload_key: str = None
    list_fetch_key: str = None
    mapper: Optional[Callable] = None
    mapper_args: Optional[Dict[str, Any]] = None
    kwargs: Optional[Dict[str, Any]] = None
