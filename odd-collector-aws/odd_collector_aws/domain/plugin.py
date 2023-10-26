from typing import List, Literal, Optional

from odd_collector_sdk.domain.filter import Filter
from odd_collector_sdk.domain.plugin import Plugin
from odd_collector_sdk.types import PluginFactory
from pydantic import BaseModel, Field, validator

from odd_collector_aws.domain.dataset_config import DatasetConfig


class AwsPlugin(Plugin):
    aws_secret_access_key: Optional[str]
    aws_access_key_id: Optional[str]
    aws_region: Optional[str]
    aws_session_token: Optional[str]
    aws_account_id: Optional[str]
    profile_name: Optional[str]
    aws_role_arn: Optional[str]
    aws_role_session_name: Optional[str]
    endpoint_url: Optional[str] = None


class GluePlugin(AwsPlugin):
    type: Literal["glue"]


class DmsPlugin(AwsPlugin):
    type: Literal["dms"]


class DynamoDbPlugin(AwsPlugin):
    type: Literal["dynamodb"]
    exclude_tables: Optional[List[str]] = []


class AthenaPlugin(AwsPlugin):
    type: Literal["athena"]


class SQSPlugin(AwsPlugin):
    type: Literal["sqs"]


class DeltaTableConfig(BaseModel):
    scheme: str = Field(default="s3", alias="schema")
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


class S3DeltaPlugin(AwsPlugin):
    type: Literal["s3_delta"]
    endpoint_url: Optional[str]
    aws_storage_allow_http: Optional[bool] = False
    delta_tables: list[DeltaTableConfig]


class S3Plugin(AwsPlugin):
    type: Literal["s3"]
    endpoint_url: Optional[str] = None
    datasets: Optional[list[DatasetConfig]] = None
    dataset_config: DatasetConfig
    filename_filter: Optional[Filter] = Filter()

    @validator("datasets", pre=True)
    def validate_datasets(cls, v):
        if v:
            raise ValueError("datasets field is deprecated, use dataset_config instead")


class QuicksightPlugin(AwsPlugin):
    type: Literal["quicksight"]


class SagemakerPlugin(AwsPlugin):
    type: Literal["sagemaker"]
    aws_secret_access_key: Optional[str]
    aws_access_key_id: Optional[str]
    aws_region: Optional[str]
    aws_session_token: Optional[str]
    aws_account_id: Optional[str]
    experiments: Optional[list[str]]


class SagemakerFeaturestorePlugin(AwsPlugin):
    type: Literal["sagemaker_featurestore"]


class KinesisPlugin(AwsPlugin):
    type: Literal["kinesis"]
    aws_account_id: str


PLUGIN_FACTORY: PluginFactory = {
    "athena": AthenaPlugin,
    "dms": DmsPlugin,
    "dynamodb": DynamoDbPlugin,
    "glue": GluePlugin,
    "kinesis": KinesisPlugin,
    "quicksight": QuicksightPlugin,
    "s3": S3Plugin,
    "sagemaker_featurestore": SagemakerFeaturestorePlugin,
    "sagemaker": SagemakerPlugin,
    "sqs": SQSPlugin,
    "s3_delta": S3DeltaPlugin,
}
