from typing import Any, Dict, Iterable

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator.generators import DmsGenerator

from odd_collector_aws.domain.fetch_paginator import fetch_paginator
from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.domain.plugin import DmsPlugin

from .client import DMSClient
from .mappers.tasks import map_dms_task

MAX_RESULTS_FOR_PAGE = 100


class Adapter(AbstractAdapter):
    def __init__(self, config: DmsPlugin) -> None:
        self._dms_client = DMSClient(config)
        self._oddrn_generator = DmsGenerator(
            cloud_settings={
                "region": config.aws_region,
                "account": self._dms_client.account_id,
            }
        )

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        endpoints_nodes = self._get_endpoints_nodes_arn_dict()
        tasks = list(self._get_tasks())
        tasks_entities = [
            map_dms_task(
                task,
                {
                    "oddrn_generator": self._oddrn_generator,
                    "endpoints_arn_dict": endpoints_nodes,
                },
            )
            for task in tasks
        ]

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tasks_entities],
        )

    def _get_tasks(self) -> Iterable:
        return fetch_paginator(
            PaginatorConfig(
                op_name="describe_replication_tasks",
                parameters={},
                page_size=MAX_RESULTS_FOR_PAGE,
                list_fetch_key="ReplicationTasks",
            ),
            self._dms_client.dms,
        )

    def _get_endpoints_nodes(self) -> Iterable:
        paginator = fetch_paginator(
            PaginatorConfig(
                op_name="describe_endpoints",
                parameters={},
                page_size=MAX_RESULTS_FOR_PAGE,
                list_fetch_key="Endpoints",
            ),
            self._dms_client.dms,
        )
        return paginator

    def _get_endpoints_nodes_arn_dict(self) -> Dict[str, Any]:
        endpoint_nodes = self._get_endpoints_nodes()
        return {
            endpoint_node.get("EndpointArn"): endpoint_node
            for endpoint_node in endpoint_nodes
        }
