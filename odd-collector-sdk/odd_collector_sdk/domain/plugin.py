from typing import Optional

from pydantic_settings import BaseSettings


class Plugin(BaseSettings, extra="allow"):
    type: str
    name: str
    description: Optional[str] = None
    namespace: Optional[str] = None


Config = Plugin
