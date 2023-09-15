from datetime import datetime

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    MetadataExtension,
)

from .base_sagemaker_entity import BaseSagemakerEntity
from .source import Source
from .trial import Trial


class Experiment(BaseSagemakerEntity):
    experiment_arn: str
    experiment_name: str
    source: Source
    creation_time: datetime
    last_modified_time: datetime
    trials: list[Trial] = []

    @property
    def arn(self):
        return self.experiment_arn

    def to_data_entity(self, oddrn_generator) -> DataEntity:
        oddrn = oddrn_generator.get_oddrn_by_path("experiments")
        return DataEntity(
            oddrn=oddrn,
            name=self.experiment_name,
            metadata=self.__extract_metadata(),
            type=DataEntityType.ML_EXPERIMENT,
            data_entity_group=DataEntityGroup(entities_list=[], group_oddrn=oddrn),
        )

    def __extract_metadata(self):
        schema = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/Trial"

        metadata = {
            "ExperimentArn": self.experiment_arn,
            "CreationTime": self.creation_time,
            "LastModifiedTime": self.last_modified_time,
        }

        return [MetadataExtension(schema_url=schema, metadata=metadata)]
