from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class MetabasePathModel(BasePathsModel):
    collections: str = ""
    dashboards: Optional[str]
    cards: Optional[str]

    @classmethod
    def _dependencies_map_factory(cls):
        return {
            "collections": ("collections",),
            "dashboards": (
                "collections",
                "dashboards",
            ),
            "cards": (
                "collections",
                "cards",
            ),
        }

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: MetabasePathModel._dependencies_map_factory()
    )


class MetabaseGenerator(Generator):
    source = "metabase"
    paths_model = MetabasePathModel
    server_model = HostnameModel
