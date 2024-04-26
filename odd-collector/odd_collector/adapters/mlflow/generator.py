from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class MlflowPathsModel(BasePathsModel):
    experiments: Optional[str]
    runs: Optional[str]
    models: Optional[str]
    model_versions: Optional[str]

    @classmethod
    def _dependencies_map_factory(cls):
        return {
            "experiments": ("experiments",),
            "runs": ("experiments", "runs"),
            "models": ("models",),
            "model_versions": ("models", "model_versions"),
        }

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: MlflowPathsModel._dependencies_map_factory()
    )


class MlFlowGenerator(Generator):
    source = "mlflow"
    server_model = HostnameModel
    paths_model = MlflowPathsModel
