from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class ModePathModel(BasePathsModel):
    reports: str = ""

    @classmethod
    def _dependencies_map_factory(cls):
        return {"reports": ("reports",)}

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: ModePathModel._dependencies_map_factory()
    )


class ModeGenerator(Generator):
    source = "mode"
    paths_model = ModePathModel
    server_model = HostnameModel
