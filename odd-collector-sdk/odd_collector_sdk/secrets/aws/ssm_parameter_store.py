import boto3
from yaml import safe_load

from odd_collector_sdk.logger import logger
from odd_collector_sdk.secrets.base_secrets import BaseSecretsBackend


class SSMParameterStoreSecretsBackend(BaseSecretsBackend):
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.region_name = kwargs.get("region_name", "us-east-1")
        self.collector_id = kwargs.get("collector_id", "")
        self.config_prefix = kwargs.get("config_prefix", "/odd/collector_config")
        self.collector_settings_section_prefix = kwargs.get(
            "collector_settings_section_prefix", "/collector_settings"
        )
        self.plugins_section_prefix = kwargs.get("plugins_section_prefix", "/plugins")
        self.ssm_client = boto3.client("ssm", region_name=self.region_name)

    @staticmethod
    def _ensure_leading_slash(secret_name: str) -> str:
        """
        In AWS SSM Parameter Store leading "/" is a mandatory naming convention.
        Function ensures path starts with "/", and can be applied for validation of path sections.

        Parameters:
            secret_name: secret parameter name.
        """
        return secret_name if secret_name.startswith("/") else f"/{secret_name}"

    @property
    def base_secret_prefix(self) -> str:
        config_prefix = self._ensure_leading_slash(self.config_prefix)
        collector_id = (
            self._ensure_leading_slash(self.collector_id) if self.collector_id else ""
        )
        return f"{config_prefix}{collector_id}"

    @property
    def collector_settings_prefix(self) -> str:
        section_prefix = self._ensure_leading_slash(
            self.collector_settings_section_prefix
        )
        return f"{self.base_secret_prefix}{section_prefix}"

    @property
    def plugins_prefix(self) -> str:
        section_prefix = self._ensure_leading_slash(self.plugins_section_prefix)
        return f"{self.base_secret_prefix}{section_prefix}"

    def _get_secrets_with_prefix(self, prefix: str, decrypt: bool = True) -> list[dict]:
        """
        Retrieve all secrets from AWS Systems Manager Parameter Store
        whose names start with the given prefix.

        Parameters:
            prefix: the prefix for filtering secrets.
            decrypt: whether to decrypt the secrets if they're encrypted (default is True).

        Returns:
            A list of dictionaries containing information about each secret.
            Each dictionary may include keys like 'Name', 'Type', 'Value', etc.
        """
        try:
            response = self.ssm_client.get_parameters_by_path(
                Path=prefix, WithDecryption=decrypt, Recursive=True
            )
            secrets = response.get("Parameters", [])
            return secrets
        except self.ssm_client.exceptions.ParameterNotFound as e:
            # Handle the case when the specified prefix doesn't exist
            logger.info(f"ParameterNotFound: {e}")
            return []
        except Exception as e:
            logger.info(f"Unexpected Exception: {e}")
            raise RuntimeError(f"An unexpected error occurred: {e}")

    def get_collector_settings(self) -> dict:
        """
        Unpack directly the name of secrets and thier values from raw ssm_client response
        getting the following result (example): {'platform_host_url': '', 'token': ''}
        """
        collector_settings = self._get_secrets_with_prefix(
            self.collector_settings_prefix
        )

        if collector_settings:
            result = {
                cs["Name"].rsplit("/", 1)[1]: cs["Value"] for cs in collector_settings
            }
            return result
        return {}

    def get_plugins(self) -> list[dict]:
        """
        Unpack directly the secret values from raw ssm_client response getting
        the following result (example): [{'type': '', 'name': '', 'host': ''}, ...]
        """
        plugins = self._get_secrets_with_prefix(self.plugins_prefix)

        if plugins:
            result = [safe_load(p["Value"]) for p in plugins]
            return result
        return []
