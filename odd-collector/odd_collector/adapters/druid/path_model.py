from typing import Optional

from oddrn_generator.path_models import BasePathsModel, DependenciesMap
from pydantic import Field


class DruidPathsModel(BasePathsModel):
    catalogs: Optional[str]
    schemas: Optional[str]
    tables: Optional[str]
    columns: Optional[str]

    @classmethod
    def _dependencies_map_factory(cls):
        return {
            "catalogs": ("catalogs",),
            "schemas": ("catalogs", "schemas"),
            "tables": ("catalogs", "schemas", "tables"),
            "columns": ("catalogs", "schemas", "tables", "columns"),
        }

    dependencies_map: DependenciesMap = Field(
        default_factory=lambda: DruidPathsModel._dependencies_map_factory()
    )
