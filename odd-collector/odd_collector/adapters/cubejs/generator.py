from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class CubeJsPathModel(BasePathsModel):
    cubes: str = ""

    @classmethod
    def _dependencies_map_factory(cls):
        return {"cubes": ("cubes",)}

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: CubeJsPathModel._dependencies_map_factory()
    )


class CubeJsGenerator(Generator):
    source = "cubejs"
    paths_model = CubeJsPathModel
    server_model = HostnameModel
