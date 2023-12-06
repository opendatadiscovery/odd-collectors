import boto3

from ..base_secrets import BaseSecretsBackend
from ...domain.collector_config import CollectorConfig


class SSMParameterStoreSecretsBackend(BaseSecretsBackend):
    def __init__(
        self,
        region_name: str = "us-east-1"
    ) -> None:
        super().__init__()
        self.ssm_client = boto3.client("ssm", region_name=region_name)

    def store_secret(self, name, value, secret_type="SecureString"):
        """
        Store a secret in AWS Systems Manager Parameter Store.

        Parameters:
        - name: The name of the parameter.
        - value: The value of the secret.
        - secret_type: The type of the secret (default is 'SecureString').
        """
        self.ssm_client.put_parameter(
            Name=name,
            Value=value,
            Type=secret_type,
            Overwrite=True
        )

    def get_secret(self, name, decrypt=True):
        """
        Retrieve a secret from AWS Systems Manager Parameter Store.

        Parameters:
        - name: The name of the parameter.
        - decrypt: Whether to decrypt the secret if it's encrypted (default is True).

        Returns:
        - The value of the secret.
        """
        response = self.ssm_client.get_parameter(
            Name=name,
            WithDecryption=decrypt
        )
        return response["Parameter"]["Value"]

    def get_collector_config(self) -> CollectorConfig:
        pass
