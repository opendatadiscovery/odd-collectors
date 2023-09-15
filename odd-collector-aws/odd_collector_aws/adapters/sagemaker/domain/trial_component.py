from datetime import datetime
from typing import Any, Optional, Union

from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataTransformer,
    MetadataExtension,
)
from oddrn_generator import SagemakerGenerator

from odd_collector_aws.adapters.sagemaker.domain.base_sagemaker_entity import (
    BaseSagemakerEntity,
)
from odd_collector_aws.adapters.sagemaker.domain.source import Source
from odd_collector_aws.adapters.sagemaker.utils.parse_job_name import parse_job_name
from odd_collector_aws.const import METADATA_PREFIX
from odd_collector_aws.utils import flatdict


class UserInfo(BaseSagemakerEntity):
    user_profile_arn: Optional[str]
    user_profile_name: Optional[str]
    domain_id: Optional[str]


class TrialComponentStatus(BaseSagemakerEntity):
    primary_status: str
    message: str


class MetadataProperties(BaseSagemakerEntity):
    commit_id: str
    repository: str
    generated_by: str
    project_id: str


class Parameter(BaseSagemakerEntity):
    number_value: Optional[float]
    string_value: Optional[str]

    @property
    def value(self):
        return self.number_value if self.number_value is not None else self.string_value


class Metric(BaseSagemakerEntity):
    metric_name: str
    source_arn: str
    time_stamp: datetime
    max: float
    min: float
    last: float
    count: float
    avg: float
    std_dev: float


class TrialComponent(BaseSagemakerEntity):
    trial_component_name: str
    trial_component_arn: str
    display_name: str
    source: Source
    status: TrialComponentStatus
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    creation_time: datetime
    last_modified_time: datetime
    created_by: UserInfo
    last_modified_by: UserInfo
    parameters: Optional[dict[str, Parameter]]
    input_artifacts: list[Any]
    output_artifacts: list[Any]
    metrics: list[Metric]

    @property
    def arn(self):
        return self.trial_component_arn

    @property
    def name(self):
        return parse_job_name(self.trial_component_name)

    def to_data_entity(
        self,
        oddrn_generator: SagemakerGenerator,
        inputs: list[str] = None,
        outputs: list[str] = None,
    ) -> DataEntity:
        if inputs is None:
            inputs = []
        if outputs is None:
            outputs = []
        oddrn = oddrn_generator.get_oddrn_by_path("jobs")
        return DataEntity(
            oddrn=oddrn,
            created_at=self.creation_time,
            updated_at=self.last_modified_time,
            name=self.name,
            type=DataEntityType.JOB,
            metadata=self.__extract_metadata(),
            data_transformer=DataTransformer(
                inputs=inputs,
                outputs=outputs,
            ),
        )

    def __get_parameters_dict(self) -> dict[str, Union[str, float]]:
        return {name: parameter.value for name, parameter in self.parameters.items()}

    def __get_metrics(self):
        res = {}
        for metric in self.metrics:
            for k, v in metric.__dict__.items():
                uid = f"Metric.{metric.metric_name}.{k}"
                res[uid] = v
        return res

    def __extract_metadata(self):
        schema = f"{METADATA_PREFIX}/sagemaker.json#/definitions/TrialComponent"

        meta = {**flatdict(self.source), **flatdict(self.__get_parameters_dict())}

        if self.metrics:
            meta |= self.__get_metrics()

        return [MetadataExtension(schema_url=schema, metadata=flatdict(meta))]


def add_input(trial_component: DataEntity, input_oddrn: str):
    if input_oddrn not in trial_component.data_transformer.inputs:
        trial_component.data_transformer.inputs.append(input_oddrn)


def add_output(trial_component: DataEntity, output_oddrn: str):
    if output_oddrn not in trial_component.data_transformer.outputs:
        trial_component.data_transformer.outputs.append(output_oddrn)
