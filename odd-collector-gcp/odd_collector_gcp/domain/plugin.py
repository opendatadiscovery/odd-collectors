from typing import Literal, Optional

from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory
from pydantic import BaseModel, Field

from odd_collector_gcp.adapters.gcs.domain.parameters import GCSAdapterParams
from odd_collector_gcp.domain.dataset_config import DatasetConfig


class GcpPlugin(Plugin):
    project: str


class BigQueryStoragePlugin(GcpPlugin):
    type: Literal["bigquery_storage"]
    page_size: Optional[int] = 100


class BigTablePlugin(GcpPlugin):
    type: Literal["bigtable"]
    rows_limit: Optional[int] = 10


class DeltaTableConfig(BaseModel):
    scheme: str = Field(default="gs", alias="schema")
    bucket: str
    prefix: str
    filter: Optional[Filter] = Filter()

    @property
    def path(self) -> str:
        return f"{self.scheme}://{self.bucket}/{self.prefix.strip('/')}"

    def append_prefix(self, path: str) -> "DeltaTableConfig":
        return DeltaTableConfig(
            schema=self.scheme,
            bucket=self.bucket,
            prefix=f"{self.prefix}/{path}",
            filter=self.filter,
        )

    def allow(self, name: str) -> bool:
        return self.filter.is_allowed(name)


class GCSDeltaPlugin(GcpPlugin):
    type: Literal["gcs_delta"]
    parameters: Optional[GCSAdapterParams] = None
    delta_tables: list[DeltaTableConfig]


class GCSPlugin(GcpPlugin):
    type: Literal["gcs"]
    datasets: list[DatasetConfig]
    parameters: Optional[GCSAdapterParams] = None
    filename_filter: Optional[Filter] = Filter()


PLUGIN_FACTORY: PluginFactory = {
    "bigquery_storage": BigQueryStoragePlugin,
    "bigtable": BigTablePlugin,
    "gcs": GCSPlugin,
    "gcs_delta": GCSDeltaPlugin,
}
