from datetime import datetime

from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    MetadataExtension,
)
from pydantic import AnyUrl

from odd_collector_aws.const import METADATA_PREFIX

from .base_sagemaker_entity import BaseSagemakerEntity
from .trial_component import TrialComponent


class Trial(BaseSagemakerEntity):
    trial_arn: str
    trial_name: str
    creation_time: datetime
    last_modified_time: datetime
    trial_components: list[TrialComponent] = []

    @property
    def arn(self):
        return self.trial_arn

    def to_data_entity(self, oddrn_generator) -> DataEntity:
        oddrn = oddrn_generator.get_oddrn_by_path("trials")
        return DataEntity(
            oddrn=oddrn,
            name=self.trial_name,
            metadata=self._extract_metadata(),
            type=DataEntityType.ML_EXPERIMENT,
            data_entity_group=DataEntityGroup(
                entities_list=[],
                group_oddrn=oddrn,
            ),
        )

    def _extract_metadata(self):
        schema: AnyUrl = f"{METADATA_PREFIX}/sagemaker.json#/definitions/Trial"

        metadata = {
            "TrialArn": self.trial_arn,
            "CreationTime": self.creation_time,
            "LastModifiedTime": self.last_modified_time,
        }

        return [MetadataExtension(schema_url=schema, metadata=metadata)]
