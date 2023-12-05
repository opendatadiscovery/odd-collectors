from typing import Optional

from pydantic import BaseSettings, Extra


class Plugin(BaseSettings, extra=Extra.allow):
    type: str
    name: str
    description: Optional[str] = None
    namespace: Optional[str] = None


Config = Plugin
