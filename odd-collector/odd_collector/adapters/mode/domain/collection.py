from typing import Any, Dict, Optional, Union

from pydantic import BaseModel

from ..generator import Generator


class Collection(BaseModel):
    token: str
    id: int
    space_type: str
    name: str
    state: str
    restricted: bool
    free_default: str
    viewable: Union[str, bool]
    links: dict

    description: Optional[str] = None
    viewed: Optional[str] = None
    default_access_level: Optional[str] = None

    @staticmethod
    def from_response(response: Dict[str, Any]):
        response["links"] = response.pop("_links")
        response["viewable"] = response.pop("viewable?")
        return Collection.model_validate(response)

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("datasource", self.id)
        return oddrn_generator.get_oddrn_by_path("datasource")
