from dataclasses import dataclass

import boto3
from botocore.client import BaseClient
from odd_collector_aws.domain.plugin import AwsPlugin
from odd_collector_aws.logger import logger


@dataclass
class TempCredentials:
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


class Aws:
    config: AwsPlugin

    def __init__(self, config: AwsPlugin):
        self.config = config

    def get_client(self, client_name: str) -> BaseClient:
        return self.create_session().client(client_name)

    def get_s3_client(self) -> BaseClient:
        return self.create_session().client("s3", endpoint_url=self.config.endpoint_url)

    def create_session(self) -> boto3.Session:
        config = self.config

        # Add invariant
        session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            aws_session_token=config.aws_session_token,
            region_name=config.aws_region,
            profile_name=config.profile_name,
        )

        if config.aws_role_arn:
            temp_credentials = self.get_temporary_credentials(session)
            session = boto3.Session(
                aws_access_key_id=temp_credentials.aws_access_key_id,
                aws_secret_access_key=temp_credentials.aws_secret_access_key,
                aws_session_token=temp_credentials.aws_session_token,
                region_name=config.aws_region,
            )

        return session

    def get_account_id(self):
        if self.config.aws_account_id:
            return self.config.aws_account_id
        else:
            return self.create_session().client("sts").get_caller_identity()["Account"]

    def get_temporary_credentials(self, session: boto3.Session) -> TempCredentials:
        """

        @param session:
        @return: TempCredentials
        """
        logger.debug(f"Assume role for {self.config.aws_role_arn}")
        sts_client = session.client("sts")
        response = sts_client.assume_role(
            RoleArn=self.config.aws_role_arn,
            RoleSessionName=self.config.aws_role_session_name,
        )
        logger.debug(f"Assumed role for {self.config.aws_role_arn}")
        temp_credentials = response["Credentials"]

        return TempCredentials(
            aws_access_key_id=temp_credentials["AccessKeyId"],
            aws_secret_access_key=temp_credentials["SecretAccessKey"],
            aws_session_token=temp_credentials["SessionToken"],
        )


class AwsClient:
    session: boto3.Session

    def __init__(self, config: AwsPlugin):
        self._config = config

        self._init_session()

    def _init_session(self):
        self.session = boto3.Session(
            aws_access_key_id=self._config.aws_access_key_id,
            aws_secret_access_key=self._config.aws_secret_access_key,
            aws_session_token=self._config.aws_session_token,
            region_name=self._config.aws_region,
        )

        if self._config.aws_role_arn and self._config.aws_role_session_name:
            try:
                assumed_role_response = self.session.client("sts").assume_role(
                    RoleArn=self._config.aws_role_arn,
                    RoleSessionName=self._config.aws_role_session_name,
                )

                if assumed_role_response.Credentials:
                    self.session = boto3.Session(
                        aws_access_key_id=assumed_role_response.Credentials.AccessKeyId,
                        aws_secret_access_key=assumed_role_response.Credentials.SecretAccessKey,
                        aws_session_token=assumed_role_response.Credentials.SessionToken,
                        region_name=self._config.aws_region,
                    )
            except Exception:
                logger.debug("Error assuming AWS Role", exc_info=True)

    def get_client(self, service_name: str) -> BaseClient:
        return self.session.client(service_name, endpoint_url=self._config.endpoint_url)

    def get_account_id(self):
        if self._config.aws_account_id:
            return self._config.aws_account_id
        else:
            return self.session.client("sts").get_caller_identity()["Account"]

    def get_region(self):
        return self._config.aws_region
