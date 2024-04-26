from typing import Optional

from oddrn_generator.generators import Generator
from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class KafkaPathsModel(BasePathsModel):
    clusters: str
    topics: Optional[str]
    columns: Optional[str]

    @classmethod
    def _dependencies_map_factory(cls):
        return {
            "clusters": ("clusters",),
            "topics": ("clusters", "topics"),
            "columns": ("clusters", "topics", "columns"),
        }

    data_source_path: str = "clusters"
    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: KafkaPathsModel._dependencies_map_factory()
    )


class KafkaGenerator(Generator):
    source = "kafka"
    paths_model = KafkaPathsModel
    server_model = HostnameModel
