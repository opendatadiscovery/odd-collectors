import os
from typing import Union

import boto3
import requests
from odd_collector_sdk.logger import logger
from odd_collector_sdk.secrets.base_secrets import BaseSecretsBackend
from yaml import safe_load


class AWSSystemsManagerParameterStoreBackend(BaseSecretsBackend):
    def __init__(self, **kwargs) -> None:
        super().__init__()

        collector_settings_parameter_name = kwargs.get(
            "collector_settings_parameter_name",
            "/odd/collector_config/collector_settings",
        )
        collector_plugins_prefix = kwargs.get(
            "collector_plugins_prefix", "/odd/collector_config/plugins"
        )
        aws_region = kwargs.get("region_name")

        self._region_name = self._get_aws_region(aws_region)
        self._collector_settings_parameter_name = self._ensure_leading_slash(
            collector_settings_parameter_name
        )
        self._collector_plugins_prefix = self._ensure_leading_slash(
            collector_plugins_prefix
        )
        self._ssm_client = boto3.client("ssm", region_name=self._region_name)
        logger.info(
            f"Successfully created boto3 SSM client with region = {self._region_name}"
        )

    @staticmethod
    def _get_aws_region(config_aws_region: str) -> Union[str, None]:
        """
        This method attempts to retrieve AWS region information with the following priority:
        1) getting region from environment variables;
        2) getting region from collector configuration .yaml file;
        3) getting region from IMDS.

        Parameters:
            config_aws_region: the region provided from collector configuration .yaml file.

        Returns:
            The AWS region, or None if any of retrieving methods worked.
        """
        env_aws_region = os.getenv("AWS_REGION")
        if env_aws_region:
            logger.info(
                "Successfully got AWS region information from evironment variable 'AWS_REGION'. "
                f"Region = {env_aws_region}."
            )
            return env_aws_region
        logger.info(
            "No 'AWS_REGION' evironment variable was provided. "
            "Attempting to get region from config."
        )

        if config_aws_region is not None:
            logger.info(
                "Successfully got AWS region information from config. "
                f"Region = {config_aws_region}."
            )
            return config_aws_region
        logger.info(
            "No region_name variable was provided in config. "
            "Attempting to retrieve region from IMDS."
        )

        try:
            # Token is required for IMDSv2
            token_response = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            )
            token = token_response.text

            # Fetch the region
            region_response = requests.get(
                "http://169.254.169.254/latest/meta-data/placement/region",
                headers={"X-aws-ec2-metadata-token": token},
            )
            imds_aws_region = region_response.text
            logger.info(
                "Successfully got AWS region information from IMDS. "
                f"Region = {imds_aws_region}."
            )
            return imds_aws_region
        except requests.RequestException as e:
            logger.info(f"Failed to retrieve AWS region dynamically from IMDS: {e}")
        logger.info("No AWS region information was retrieved. Region = None")
        return None

    @staticmethod
    def _ensure_leading_slash(secret_name: str) -> str:
        """
        In AWS SSM Parameter Store leading "/" is a mandatory naming convention.
        Function ensures naming path starts with "/".

        Parameters:
            secret_name: secret parameter name.
        """
        return secret_name if secret_name.startswith("/") else f"/{secret_name}"

    def _get_secret_by_name(self, name: str, decrypt: bool = True) -> dict:
        """
        Retrieve a secret from AWS Systems Manager Parameter Store by name.

        Parameters:
            name: the name of the parameter.
            decrypt: whether to decrypt the secret if it's encrypted (default is True).

        Returns:
            A dictionary containing information about the secret.
            The dictionary may include keys like 'Name', 'Type', 'Value', etc.
        """
        try:
            response = self._ssm_client.get_parameter(Name=name, WithDecryption=decrypt)
            secret = response.get("Parameter", {})
            return secret
        except self._ssm_client.exceptions.ParameterNotFound as e:
            # Handle the case when the specified parameter doesn't exist
            # TODO: update logging with information about what exectly paramter we tried to find (name)
            logger.info(f"ParameterNotFound: {e}")
            return {}

    def _get_secrets_by_prefix(self, prefix: str, decrypt: bool = True) -> list[dict]:
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
            response = self._ssm_client.get_parameters_by_path(
                Path=prefix, WithDecryption=decrypt, Recursive=True
            )
            secrets = response.get("Parameters", [])
            return secrets
        except self._ssm_client.exceptions.ParameterNotFound as e:
            # Handle the case when the specified prefix doesn't exist
            # TODO: update logging with information about what exectly paramter we tried to find (name)
            logger.info(f"ParameterNotFound: {e}")
            return []

    def get_collector_settings(self) -> dict:
        """
        Unpack directly the secret values from raw ssm_client response getting
        the following result (example): {'platform_host_url': '', 'token': ''}
        """
        collector_settings = self._get_secret_by_name(
            self._collector_settings_parameter_name
        )

        if collector_settings:
            return safe_load(collector_settings["Value"])
        return {}

    def get_plugins(self) -> list[dict]:
        """
        Unpack directly the secret values from raw ssm_client response getting
        the following result (example): [{'type': '', 'name': '', 'host': ''}, ...]
        """
        plugins = self._get_secrets_by_prefix(self._collector_plugins_prefix)

        if plugins:
            return [safe_load(p["Value"]) for p in plugins]
        return []
