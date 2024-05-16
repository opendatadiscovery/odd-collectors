from typing import Optional

from oddrn_generator.generators import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import AWSCloudModel
from pydantic import Field


class SqsPathsModel(BasePathsModel):
    queue: Optional[str]

    @classmethod
    def _dependencies_map_factory(cls):
        return {"queue": ("queue",)}

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: SqsPathsModel._dependencies_map_factory()
    )


class SqsGenerator(Generator):
    source = "sqs"
    paths_model = SqsPathsModel
    server_model = AWSCloudModel
