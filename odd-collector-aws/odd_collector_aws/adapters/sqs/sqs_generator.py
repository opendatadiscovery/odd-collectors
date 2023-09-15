from typing import Optional

from oddrn_generator.generators import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import AWSCloudModel


class SqsPathsModel(BasePathsModel):
    queue: Optional[str]

    class Config:
        dependencies_map = {"queue": ("queue",)}


class SqsGenerator(Generator):
    source = "sqs"
    paths_model = SqsPathsModel
    server_model = AWSCloudModel
