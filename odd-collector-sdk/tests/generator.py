from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class TestPathModel(BasePathsModel):
    databases: Optional[str]

    class Config:
        dependencies_map = {
            "databases": ("databases",),
        }


class TestGenerator(Generator):
    source = "test"
    paths_model = TestPathModel
    server_model = HostnameModel
