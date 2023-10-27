import traceback
from typing import Iterable, Optional, Union

from funcy import lflatten
from odd_collector_aws.adapters.s3.file_system import FileSystem
from odd_collector_aws.adapters.sagemaker.domain import (
    Association,
    Experiment,
    Trial,
    TrialComponent,
)
from odd_collector_aws.adapters.sagemaker.domain.artifact import (
    Artifact,
    DataAsset,
    create_image,
    create_model,
)
from odd_collector_aws.adapters.sagemaker.logger import logger
from odd_collector_aws.aws.aws_client import AwsClient
from odd_collector_aws.domain.paginator_config import PaginatorConfig
from odd_collector_aws.domain.plugin import SagemakerPlugin
from odd_collector_aws.utils.parse_s3_url import parse_s3_url
from oddrn_generator import S3Generator


class SagemakerClient:
    def __init__(self, config: SagemakerPlugin):
        aws_client = AwsClient(config)
        self.client = aws_client.get_client("sagemaker")
        self.account_id = aws_client.get_account_id()
        self.s3_fs = FileSystem(config)

    def get_experiments(
        self, experiments_name: Optional[list[str]]
    ) -> Iterable[Experiment]:
        pconf = PaginatorConfig(
            op_name="search",
            list_fetch_key="Results",
            kwargs={
                "Resource": "Experiment",
                **self._get_search_expression(experiments_name),
            },
        )
        for experiment in self._fetch(pconf):
            experiment = experiment.get("Experiment")
            trials = self.get_trials(experiment.get("ExperimentName"))

            yield Experiment.parse_obj({**experiment, "Trials": trials})

    def get_trials(self, experiment_name: str) -> Iterable[Trial]:
        pconf = PaginatorConfig(
            op_name="list_trials",
            list_fetch_key="TrialSummaries",
            kwargs={"ExperimentName": experiment_name},
        )

        for trial in self._fetch(pconf):
            trial_components = self.get_trial_components(trial.get("TrialName"))
            yield Trial.parse_obj({**trial, "TrialComponents": trial_components})

    def get_trial_components(self, trial_name: str) -> Iterable[TrialComponent]:
        pconf = PaginatorConfig(
            op_name="list_trial_components",
            parameters={},
            list_fetch_key="TrialComponentSummaries",
            kwargs={"TrialName": trial_name},
        )

        for trial_component in self._fetch(pconf):
            yield self.get_trial_component_description(
                trial_component.get("TrialComponentName")
            )

    def get_trial_component_description(
        self, trial_component_name: str
    ) -> TrialComponent:
        description = self.client.describe_trial_component(
            TrialComponentName=trial_component_name
        )

        arn = description.get("TrialComponentArn")
        return TrialComponent.parse_obj(
            {
                **description,
                "InputArtifacts": self._get_input_artifacts(arn),
                "OutputArtifacts": self._get_output_artifacts(arn),
            }
        )

    def _is_artifact(self, entity_type: str):
        return entity_type == "artifact"

    def _to_artifact(
        self, arn: str, association_type: str
    ) -> list[Union[DataAsset, Artifact]]:
        arn_name = arn.split(":")[5]
        entity_type = arn_name.split("/")[0]

        if not self._is_artifact(entity_type):
            return []

        artifact = self._describe_artifact(arn)
        s3_url: str = artifact["Source"]["SourceUri"]

        if association_type == "DataSet":
            try:
                return [DataAsset(oddrn=self.get_dataset_oddrn(artifact))]
            except Exception as e:
                logger.error(e)
                logger.debug(traceback.format_exc())
                return []
        if association_type == "Image":
            return [create_image(uri=s3_url)]
        if association_type == "Model":
            return [create_model(uri=s3_url, arn=artifact["ArtifactArn"])]

        return []

    def _describe_artifact(self, arn: str):
        try:
            return self.client.describe_artifact(ArtifactArn=arn)
        except Exception as e:
            logger.warning(e, exc_info=True)

    def _get_input_artifacts(self, arn: str):
        return lflatten(
            self._to_artifact(assoc.source_arn, assoc.source_type)
            for assoc in self._get_associations(dest_arn=arn)
        )

    def _get_output_artifacts(self, arn: str):
        return lflatten(
            self._to_artifact(assoc.destination_arn, assoc.destination_type)
            for assoc in self._get_associations(src_arn=arn)
        )

    def _get_associations(
        self, src_arn: Optional[str] = None, dest_arn: Optional[str] = None
    ) -> list[Association]:
        kwargs = {"SourceArn": src_arn} if src_arn else {"DestinationArn": dest_arn}
        pconf = PaginatorConfig(
            op_name="list_associations",
            parameters={},
            list_fetch_key="AssociationSummaries",
            kwargs=kwargs,
        )
        return [Association.parse_obj(resp) for resp in self._fetch(pconf)]

    def _fetch(self, conf: PaginatorConfig):
        paginator = self.client.get_paginator(conf.op_name)

        for res in paginator.paginate(**conf.kwargs):
            yield from res.get(conf.list_fetch_key)

    @staticmethod
    def _get_search_expression(experiments_name: Optional[list[str]]):
        if experiments_name:
            return {
                "SearchExpression": {
                    "Filters": [
                        {
                            "Name": "ExperimentName",
                            "Operator": "In",
                            "Value": ",".join(experiments_name),
                        }
                    ]
                }
            }

        return {}

    def get_dataset_oddrn(self, artifact) -> str:
        bucket, key = parse_s3_url(artifact.get("Source").get("SourceUri"))
        generator = S3Generator(buckets=bucket, keys=key)
        return generator.get_oddrn_by_path("keys")
