from typing import Literal, Optional

from odd_collector_azure.adapters.blob_storage.dataset_config import DatasetConfig
from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory
from pydantic import SecretStr, validator


class AzurePlugin(Plugin):
    client_id: str  # client_id of registered in AD app
    client_secret: str  # client secret of registered in AD app
    username: str
    password: str
    domain: str  # yourdomain.com


class PowerBiPlugin(AzurePlugin):
    type: Literal["powerbi"]


class AzureSQLPlugin(Plugin):
    type: Literal["azure_sql"]
    database: str
    server: str
    port: str
    username: str
    password: str
    encrypt: str = "yes"
    trust_server_certificate: str = "no"
    connection_timeout: str = "30"


class BlobPlugin(Plugin):
    type: Literal["blob_storage"]
    account_name: str
    account_key: Optional[SecretStr]
    connection_string: Optional[SecretStr]
    file_filter: Optional[Filter] = Filter()
    dataset_config: DatasetConfig
    datasets: Optional[list[DatasetConfig]] = None

    @validator("datasets", pre=True)
    def validate_datasets(cls, v):
        if v:
            raise ValueError("datasets field is deprecated, use dataset_config instead")


class DataFactoryPlugin(Plugin):
    type: Literal["azure_data_factory"]
    subscription: str
    resource_group: str
    factory: str
    pipeline_filter: Filter = Filter()


PLUGIN_FACTORY: PluginFactory = {
    "powerbi": PowerBiPlugin,
    "azure_sql": AzureSQLPlugin,
    "blob_storage": BlobPlugin,
    "azure_data_factory": DataFactoryPlugin,
}
