from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class TestPathModel(BasePathsModel):
    databases: Optional[str] = None

    @classmethod
    def _dependencies_map_factory(cls):
        return {
            "databases": ("databases",),
        }

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: TestPathModel._dependencies_map_factory()
    )


class TestGenerator(Generator):
    source = "test"
    paths_model = TestPathModel
    server_model = HostnameModel
