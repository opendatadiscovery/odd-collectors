from typing import Literal

from pydantic import BaseSettings, Extra

from odd_collector_sdk.secrets.aws.ssm_parameter_store import AWSSystemsManagerParameterStoreBackend
from odd_collector_sdk.secrets.base_secrets import BaseSecretsBackend

PROVIDERS = {
    "AWSSystemsManagerParameterStore": AWSSystemsManagerParameterStoreBackend,
}


class SecretsBackendSettings(BaseSettings, extra=Extra.allow):
    provider: Literal["AWSSystemsManagerParameterStore"]


class SecretsBackendFactory:
    def __init__(self, settings: SecretsBackendSettings) -> None:
        self._settings = settings
        self._provider: BaseSecretsBackend = PROVIDERS[settings.provider](**settings.dict(exclude={"provider"}))

    def create_provider(self) -> BaseSecretsBackend:
        return PROVIDERS[self._settings.provider](**self._settings.dict(exclude={"provider"}))
