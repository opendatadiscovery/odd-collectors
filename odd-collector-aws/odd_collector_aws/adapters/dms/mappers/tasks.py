import os
from datetime import datetime
from json import loads
from typing import Any, Dict, List, Type, Union

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    DataTransformerRun,
    JobRunStatus,
)
from oddrn_generator.generators import DmsGenerator
from yaml import safe_load

from odd_collector_aws.adapters.dms import (
    _METADATA_SCHEMA_URL_PREFIX,
    _keys_to_include_task,
)

from .endpoints import EndpointEngine, engines_factory
from .metadata import create_metadata_extension_list
from .tables import EntitiesExtractor

DMS_TASK_STATUSES: Dict[str, JobRunStatus] = {
    "creating": JobRunStatus.UNKNOWN,
    "running": JobRunStatus.RUNNING,
    "stopped": JobRunStatus.ABORTED,
    "stopping": JobRunStatus.UNKNOWN,
    "deleting": JobRunStatus.UNKNOWN,
    "failed": JobRunStatus.FAILED,
    "starting": JobRunStatus.UNKNOWN,
    "ready": JobRunStatus.UNKNOWN,
    "modifying": JobRunStatus.UNKNOWN,
    "moving": JobRunStatus.UNKNOWN,
    "failed-move": JobRunStatus.BROKEN,
}


class IOTransformer:
    def __init__(
        self,
        endpoints_arn_dict: Dict[str, Dict[str, Any]],
        raw_job_data: Dict[str, Any],
        factory: Dict[str, Type[EndpointEngine]],
        rules_nodes: List[Dict[str, Any]],
    ):
        self.rules_nodes = rules_nodes
        self.factory = factory
        self.raw_job_data = raw_job_data
        self.endpoints_arn_dict = endpoints_arn_dict

    @property
    def __input_endpoint_node(self) -> Dict[str, Any]:
        return self.__get_endpoint_node("SourceEndpointArn")

    @property
    def __output_endpoint_node(self) -> Dict[str, Any]:
        return self.__get_endpoint_node("TargetEndpointArn")

    def __get_endpoint_node(self, arn_node_name: str) -> Dict[str, Any]:
        return self.endpoints_arn_dict.get(self.raw_job_data.get(arn_node_name))

    @staticmethod
    def __get_platform_host_url() -> str:
        config_file_name = "collector_config.yaml"
        path = (
            os.path.dirname(os.path.abspath(config_file_name)) + "/" + config_file_name
        )
        with open(path) as f:
            config_dict: Dict[str, Any] = safe_load(f)
        return config_dict["platform_host_url"]

    @property
    def __platform_host_url(self):
        return self.__get_platform_host_url()

    def __find_endpoint_engine_cls(
        self, endpoint_node: Dict[str, Any]
    ) -> Union[Type[EndpointEngine], None]:
        engine_name = endpoint_node.get("EngineName")
        return self.factory.get(engine_name)

    def __extract_oddrns(self, endpoint_node: Dict[str, Any]) -> List[str]:
        endpoint_engine_cls = self.__find_endpoint_engine_cls(endpoint_node)
        if endpoint_engine_cls is None:
            return []
        endpoint_engine = endpoint_engine_cls(endpoint_node)
        extractor = EntitiesExtractor(
            self.rules_nodes, self.__get_platform_host_url(), endpoint_engine
        )
        return extractor.get_oddrns_list()

    def extract_input_oddrns(self):
        return self.__extract_oddrns(self.__input_endpoint_node)

    def extract_output_oddrns(self):
        return self.__extract_oddrns(self.__output_endpoint_node)


def map_dms_task(
    raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    endpoints_arn_dict: Dict[str, Dict[str, Any]] = mapper_args["endpoints_arn_dict"]
    rules_nodes: List[Dict[str, Any]] = loads(raw_job_data["TableMappings"])["rules"]

    io_transformer = IOTransformer(
        endpoints_arn_dict, raw_job_data, engines_factory, rules_nodes
    )

    trans = DataTransformer(
        inputs=io_transformer.extract_input_oddrns(),
        outputs=io_transformer.extract_output_oddrns(),
    )
    data_entity_task = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path(
            "tasks", raw_job_data["ReplicationTaskIdentifier"]
        ),
        name=raw_job_data["ReplicationTaskIdentifier"],
        owner=None,
        type=DataEntityType.JOB,
        created_at=raw_job_data.get("ReplicationTaskCreationDate"),
    )
    data_entity_task.data_transformer = trans
    data_entity_task.metadata = create_metadata_extension_list(
        _METADATA_SCHEMA_URL_PREFIX, raw_job_data, _keys_to_include_task
    )
    return data_entity_task


def map_dms_task_run(
    raw_job_data: Dict[str, Any], mapper_args: Dict[str, Any]
) -> DataEntity:
    oddrn_generator: DmsGenerator = mapper_args["oddrn_generator"]
    oddrn_generator.get_oddrn_by_path(
        "tasks", raw_job_data["ReplicationTaskIdentifier"]
    )
    status = DMS_TASK_STATUSES.get(raw_job_data["Status"], JobRunStatus.UNKNOWN)
    data_entity_task_run = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("runs", "run"),
        name=raw_job_data["ReplicationTaskIdentifier"] + "_run",
        owner=None,
        type=DataEntityType.JOB_RUN,
        created_at=raw_job_data.get("ReplicationTaskCreationDate"),
        data_transformer_run=DataTransformerRun(
            # start_time=raw_job_data.get('ReplicationTaskStartDate'),
            start_time=datetime(2022, 9, 24),
            end_time=datetime(2022, 9, 25),
            transformer_oddrn=oddrn_generator.get_oddrn_by_path("tasks"),
            status_reason=(
                raw_job_data.get("StopReason") if status == "failed" else None
            ),
            status=status,
        ),
    )
    return data_entity_task_run
