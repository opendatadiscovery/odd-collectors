import datetime
from cmath import log
from typing import Any, Dict, List

import boto3
import pytz
from botocore.exceptions import ClientError
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList, DataEntityType

from odd_collector_aws.domain.plugin import SQSPlugin

from .sqs_generator import SqsGenerator

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/sqs.json"
)


class Adapter(AbstractAdapter):
    def __init__(self, config: SQSPlugin) -> None:
        self._account_id = boto3.client(
            "sts",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        ).get_caller_identity()["Account"]
        self._sqs_client = boto3.client(
            "sqs",
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )

        self.__oddrn_generator = SqsGenerator(
            cloud_settings={"region": config.aws_region, "account": self._account_id}
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def get_data_entities(self) -> List[DataEntity]:
        try:
            sqs_queues = []
            for queue in self._sqs_client.list_queues()["QueueUrls"]:
                queue_name = queue.split("/")[-1]
                queue_attributes = self._sqs_client.get_queue_attributes(
                    QueueUrl=queue, AttributeNames=["All"]
                )["Attributes"]
                created_at = datetime.datetime.fromtimestamp(
                    int(queue_attributes["CreatedTimestamp"]), tz=pytz.utc
                )
                updated_at = datetime.datetime.fromtimestamp(
                    int(queue_attributes["LastModifiedTimestamp"]), tz=pytz.utc
                )
                del queue_attributes["CreatedTimestamp"]
                del queue_attributes["LastModifiedTimestamp"]
                del queue_attributes["Policy"]
                queue_attributes["AccountID"] = self._account_id
                metadata = [
                    {
                        "schema_url": (
                            f"{SCHEMA_FILE_URL}#/definitions/SQSDataSetExtension"
                        ),
                        "metadata": queue_attributes,
                    }
                ]
                sqs_queues.append(
                    DataEntity(
                        name=queue_name,
                        oddrn=self.__oddrn_generator.get_oddrn_by_path(
                            "queue", queue_name
                        ),
                        type=DataEntityType.KAFKA_TOPIC,
                        created_at=created_at,
                        updated_at=updated_at,
                        metadata=metadata,
                        dataset={"field_list": []},
                    )
                )
        except ClientError:
            raise
        else:
            return sqs_queues

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()
