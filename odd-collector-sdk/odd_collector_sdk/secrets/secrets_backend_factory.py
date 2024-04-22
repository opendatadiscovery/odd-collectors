from typing import Literal

from odd_collector_sdk.secrets.aws.ssm_parameter_store import (
    AWSSystemsManagerParameterStoreBackend,
)
from odd_collector_sdk.secrets.base_secrets import BaseSecretsBackend
from pydantic_settings import BaseSettings

PROVIDERS = {
    "AWSSystemsManagerParameterStore": AWSSystemsManagerParameterStoreBackend,
}


class SecretsBackendSettings(BaseSettings, extra="allow"):
    provider: Literal["AWSSystemsManagerParameterStore"]


class SecretsBackendFactory:
    def __init__(self, settings: SecretsBackendSettings) -> None:
        self._settings = settings

    def get_provider(self) -> BaseSecretsBackend:
        return PROVIDERS[self._settings.provider](
            **self._settings.dict(exclude={"provider"})
        )
