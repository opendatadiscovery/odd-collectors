import abc
from abc import ABC
from typing import List, Optional

import flatdict
from odd_models import DataTransformer
from odd_models.models import (
    DataConsumer,
    DataEntity,
    DataEntityType,
    DataSet,
    MetadataExtension,
)
from oddrn_generator import Generator
from pydantic import BaseModel

from .base_sagemaker_entity import BaseSagemakerEntity


class Association(BaseSagemakerEntity):
    source_arn: str
    source_type: str
    destination_arn: str
    destination_type: str


class DataAsset(BaseModel):
    oddrn: str


class Artifact(BaseSagemakerEntity, ABC):
    artifact_type: str
    media_type: Optional[str] = None
    arn: Optional[str] = None
    name: str
    uri: str

    def get_name(self) -> str:
        name = self.arn or self.uri
        return name.split("/")[-1]

    @abc.abstractmethod
    def to_data_entity(self, *args, **kwargs) -> DataEntity:
        raise NotImplementedError

    def _extract_metadata(self):
        schema = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/sagemaker.json#/definitions/TrialComponent"

        m = {
            "Uri": self.uri,
            "ArtifactType": self.artifact_type,
            "MediaType": self.media_type,
            "Arn": self.arn,
            "Name": self.name,
        }

        return [MetadataExtension(schema_url=schema, metadata=flatdict.FlatDict(m))]


class UnknownDatasetArtifact(Artifact):
    def to_data_entity(self, oddrn_generator: Generator) -> DataEntity:
        oddrn = oddrn_generator.get_oddrn_by_path("keys")
        return DataEntity(
            name=self.name,
            oddrn=oddrn,
            metadata=None,
            updated_at=None,
            created_at=None,
            type=DataEntityType.FILE,
            dataset=DataSet(field_list=[]),
        )


class Image(Artifact):
    def to_data_entity(
        self, oddrn_generator: Generator, trial_component_oddrn: Optional[str]
    ) -> DataEntity:
        oddrn_generator.set_oddrn_paths(artifacts=self.name)

        if trial_component_oddrn is None:
            outputs: List[str] = []
        else:
            outputs: List[str] = [trial_component_oddrn]
        return DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("artifacts"),
            name=self.name,
            type=DataEntityType.MICROSERVICE,
            metadata=self._extract_metadata(),
            data_transformer=DataTransformer(inputs=[], outputs=outputs),
        )


class Model(Artifact):
    def to_data_entity(
        self, oddrn_generator: Generator, trial_component_oddrn: Optional[str]
    ) -> DataEntity:
        arn_id = self.arn.split("/")[-1]
        oddrn_generator.set_oddrn_paths(artifacts=f"{self.name}:{arn_id}")

        if trial_component_oddrn is None:
            inputs = []
        else:
            inputs = [trial_component_oddrn]

        return DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("artifacts"),
            name=self.name,
            type=DataEntityType.ML_MODEL_TRAINING,
            metadata=self._extract_metadata(),
            data_consumer=DataConsumer(inputs=inputs),
        )


def create_image(uri: str) -> Image:
    return Image(
        Name=uri.split("/")[-1],
        Arn=uri,
        Uri=uri,
        ArtifactType="Image",
    )


def create_model(uri: str, arn: str) -> Model:
    return Model(Name="model", Uri=uri, ArtifactType="Model", Arn=arn)


# def create_dummy_dataset_artifact(uri: str, arn: str) -> UnknownDatasetArtifact:
#     bucket, key = parse_s3_url(uri)
#     name = escape(key)
#     return UnknownDatasetArtifact(Name=name, Uri=uri, Arn=arn, ArtifactType="Dataset")


def as_input(data_entity: DataEntity, trial_component_oddrn: str):
    if (
        isinstance(data_entity, DataEntity)
        and data_entity.type == DataEntityType.MICROSERVICE
    ):
        if trial_component_oddrn not in data_entity.data_transformer.outputs:
            data_entity.data_transformer.outputs.append(trial_component_oddrn)


def as_output(data_entity: DataEntity, trial_component_oddrn: str):
    if (
        isinstance(data_entity, DataEntity)
        and data_entity.type == DataEntityType.ML_MODEL_TRAINING
    ):
        if trial_component_oddrn not in data_entity.data_consumer.inputs:
            data_entity.data_consumer.inputs.append(trial_component_oddrn)
