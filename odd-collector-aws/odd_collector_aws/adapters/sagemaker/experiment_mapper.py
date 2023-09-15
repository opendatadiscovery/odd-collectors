import logging
import traceback
from typing import Any, Optional, Union

from odd_models.models import DataEntity
from oddrn_generator.generators import S3Generator, SagemakerGenerator

from odd_collector_aws.adapters.sagemaker.domain.artifact import (
    Artifact,
    DataAsset,
    as_input,
    as_output,
)
from odd_collector_aws.adapters.sagemaker.domain.experiment import Experiment
from odd_collector_aws.adapters.sagemaker.domain.trial import Trial
from odd_collector_aws.adapters.sagemaker.domain.trial_component import (
    TrialComponent,
    add_input,
    add_output,
)
from odd_collector_aws.errors import MappingError


# TODO: Flyweight pattern
class DataEntityCache(object):
    def __init__(self):
        self.entities: dict[str, DataEntity] = {}

    def get(self, arn: str) -> DataEntity:
        return self.entities.get(arn)

    def save(self, arn: str, data_entity) -> None:
        self.entities[arn] = data_entity

    def get_all(self) -> list[DataEntity]:
        return list(self.entities.values())


class ExperimentMapper:
    cache: dict[str, DataEntity]
    experiment_data_entity: DataEntity = None

    def __init__(
        self,
        generator: SagemakerGenerator,
        s3_generator: S3Generator,
        cache: Optional[DataEntityCache] = None,
    ):
        self.entities = cache or DataEntityCache()

        self.generator = generator
        self.s3_generator = s3_generator

    def map_experiment(self, experiment: Experiment) -> list[DataEntity]:
        # Set oddrn context by experiment name
        try:
            self.generator.set_oddrn_paths(experiments=experiment.experiment_name)

            # Create experiment data entity
            experiment_data_entity = experiment.to_data_entity(self.generator)

            self.entities.save(experiment.arn, experiment_data_entity)

            # Map experiment trials
            for trial in experiment.trials:
                self.map_trial(trial, experiment_data_entity)

            return list(self.entities.get_all())
        except Exception as e:
            logging.error(
                f"Error while mapping experiment {experiment.experiment_name}"
            )
            logging.debug(traceback.format_exc())
            raise MappingError from e

    def map_trial(self, trial: Trial, experiment_data_entity: DataEntity) -> None:
        # Set oddrn context by trial name
        self.generator.set_oddrn_paths(trials=trial.trial_name)

        # Transform to DataEntity
        trial_data_entity = trial.to_data_entity(self.generator)

        self.entities.save(trial.arn, trial_data_entity)

        for trial_component in trial.trial_components:
            # get trial component DataEntity and Artifacts DataEntities chained with him
            self.map_trial_component(trial_component)

        experiment_data_entity.data_entity_group.entities_list.append(
            trial_data_entity.oddrn
        )

    def map_trial_component(self, trial_component: TrialComponent) -> None:
        """
        Creates DataEntity for TrialComponent and self Input/Output Artifacts.
        Returns them as list of DataEntities
        """

        # Set oddrn context by TrialComponent(Job) name
        self.generator.set_oddrn_paths(jobs=trial_component.trial_component_name)

        data_entity = trial_component.to_data_entity(self.generator)
        self.entities.save(trial_component.arn, data_entity)

        for artifact in trial_component.input_artifacts:
            self.map_input(trial_component.arn, artifact)

        for artifact in trial_component.output_artifacts:
            self.map_output(trial_component.trial_component_arn, artifact)

    def map_input(self, trial_component_arn: str, artifact: Artifact) -> None:
        trial_component_data_entity = self.entities.get(trial_component_arn)
        input_data_entity = self.map_artifact(
            artifact, trial_component_data_entity.oddrn
        )

        add_input(trial_component_data_entity, input_data_entity.oddrn)
        as_input(input_data_entity, trial_component_data_entity.oddrn)

    def map_output(
        self,
        trial_component_arn: str,
        artifact: Artifact,
    ) -> None:
        """
        Maps TrialComponent's output Artifacts
        """
        trial_component_data_entity = self.entities.get(trial_component_arn)
        output_data_entity = self.map_artifact(
            artifact, trial_component_data_entity.oddrn
        )

        add_output(trial_component_data_entity, output_data_entity.oddrn)
        as_output(output_data_entity, trial_component_data_entity.oddrn)

    def map_artifact(
        self, artifact: Any, trial_component_arn: str
    ) -> Union[DataAsset, DataEntity]:
        if isinstance(artifact, DataAsset):
            return artifact

        if de := self.entities.get(artifact.arn):
            return de

        data_entity = artifact.to_data_entity(self.generator, trial_component_arn)

        self.entities.save(artifact.arn, data_entity)
        return data_entity
