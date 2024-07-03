import logging
from typing import Type, TypeVar, cast

from botocore.exceptions import ClientError
from odd_collector_aws.aws.aws_client import Aws
from odd_collector_aws.domain.plugin import AwsPlugin, S3DeltaPlugin, S3Plugin
from odd_collector_aws.errors import AccountIdError
from oddrn_generator import Generator
from oddrn_generator.generators import S3CustomGenerator, S3Generator

T = TypeVar("T", bound=Generator)


def create_generator(generator_cls: Type[T], aws_plugin: AwsPlugin) -> T:
    aws_client = Aws(aws_plugin)

    if generator_cls == S3Generator:
        config = cast(S3Plugin, aws_plugin)

        # hotfix to manage different structure of S3Plugin and S3DeltaPlugin
        if isinstance(config, S3DeltaPlugin):
            bucket = config.delta_tables.bucket
        else:
            bucket = config.dataset_config.bucket

        if config.endpoint_url:
            return S3CustomGenerator(endpoint=config.endpoint_url, buckets=bucket)

        return generator_cls(buckets=bucket)

    account_id = aws_plugin.aws_account_id

    if not account_id:
        try:
            account_id = aws_client.get_account_id()
        except ClientError as e:
            logging.debug(e)
            raise AccountIdError from e

    return generator_cls(
        cloud_settings={"region": aws_plugin.aws_region, "account": account_id}
    )
