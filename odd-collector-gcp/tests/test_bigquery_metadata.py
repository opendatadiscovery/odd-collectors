import pytest
from attr import dataclass
from funcy import partial
from google.cloud.bigquery import (
    ExternalConfig,
    SourceFormat,
    TimePartitioning,
    TimePartitioningType,
)
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from pydantic import BaseModel

from odd_collector_gcp.adapters.bigquery_storage.mapper import BigQueryMetadataEncoder


@pytest.fixture
def partial_extract_metadata():
    return partial(
        extract_metadata,
        datasource="test_datasource",
        definition=DefinitionType.DATASET,
    )


GCS_URI = "gs://path-to-your-data-in-gcs/*.csv"
external_config = ExternalConfig(SourceFormat.CSV)
external_config.source_uris = [GCS_URI]
time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY)

COMPLEX_METADATA = {
    "meta": "meta",
    "nested_meta": {
        "nested_field_one": 1,
        "nested_field_two": "string",
        "nested_field_three": [{"foo": "bat"}, {"foo": {}}],
        "nested_field_four": {
            "more_nested_field": 1,
            "more_nested_field_two": {},
            "more_nested_field_three": "string",
            "more_nested_field_external": external_config,
            "more_nested_field_time_partitioning": time_partitioning,
        },
        "nested_field_five": {},
        "nested_field_external": external_config,
        "nested_field_time_partitioning": time_partitioning,
    },
}

FLATTEN = {
    "meta": "meta",
    "nested_meta.nested_field_one": 1,
    "nested_meta.nested_field_two": "string",
    "nested_meta.nested_field_three": [{"foo": "bat"}, {"foo": {}}],
    "nested_meta.nested_field_four.more_nested_field": 1,
    "nested_meta.nested_field_four.more_nested_field_two": {},
    "nested_meta.nested_field_four.more_nested_field_three": "string",
    "nested_meta.nested_field_four.more_nested_field_external": external_config,
    "nested_meta.nested_field_four.more_nested_field_time_partitioning": time_partitioning,
    "nested_meta.nested_field_five": {},
    "nested_field_external": external_config,
    "nested_field_time_partitioning": time_partitioning,
}

ENCODED_METADATA = {
    "meta": "meta",
    "nested_meta": {
        "nested_field_one": 1,
        "nested_field_two": "string",
        "nested_field_three": [{"foo": "bat"}, {"foo": {}}],
        "nested_field_four": {
            "more_nested_field": 1,
            "more_nested_field_two": {},
            "more_nested_field_three": "string",
            "more_nested_field_external": {
                "csv_options": {},
                "options": {},
                "schema": [],
                "source_format": "CSV",
                "source_uris": ["gs://path-to-your-data-in-gcs/*.csv"],
            },
            "more_nested_field_time_partitioning": {"type_": "DAY"},
        },
        "nested_field_five": {},
        "nested_field_external": {
            "csv_options": {},
            "options": {},
            "schema": [],
            "source_format": "CSV",
            "source_uris": ["gs://path-to-your-data-in-gcs/*.csv"],
        },
        "nested_field_time_partitioning": {"type_": "DAY"},
    },
}

FLATTEN_ENCODED_METADATA = {
    "meta": "meta",
    "nested_meta.nested_field_one": 1,
    "nested_meta.nested_field_two": "string",
    "nested_meta.nested_field_three": [{"foo": "bat"}, {"foo": {}}],
    "nested_meta.nested_field_four.more_nested_field": 1,
    "nested_meta.nested_field_four.more_nested_field_two": {},
    "nested_meta.nested_field_four.more_nested_field_three": "string",
    "nested_meta.nested_field_four.more_nested_field_external.csv_options": {},
    "nested_meta.nested_field_four.more_nested_field_external.options": {},
    "nested_meta.nested_field_four.more_nested_field_external.schema": [],
    "nested_meta.nested_field_four.more_nested_field_external.source_format": "CSV",
    "nested_meta.nested_field_four.more_nested_field_external.source_uris": [
        "gs://path-to-your-data-in-gcs/*.csv"
    ],
    "nested_meta.nested_field_four.more_nested_field_time_partitioning.type_": "DAY",
    "nested_meta.nested_field_five": {},
    "nested_field_external.csv_options": {},
    "nested_field_external.options": {},
    "nested_field_external.schema": [],
    "nested_field_external.source_format": "CSV",
    "nested_field_external.source_uris": ["gs://path-to-your-data-in-gcs/*.csv"],
    "nested_field_time_partitioning.type_": "DAY",
}


class Entity:
    def __init__(self, foo, bar):
        self.foo = foo
        self.bar = bar


class EntityWithMetadata:
    def __init__(self, foo, bar, odd_metadata):
        self.foo = foo
        self.bar = bar
        self.odd_metadata = odd_metadata


@dataclass
class DataclassEntity:
    foo: str
    bar: str


@dataclass
class DataclassEntityWithMetadata:
    foo: str
    bar: str
    odd_metadata: dict


class PydanticEntity(BaseModel):
    foo: str
    bar: str


class PydanticEntityWithMetadata(BaseModel):
    foo: str
    bar: str
    odd_metadata: dict


def test_nested_complex_types(partial_extract_metadata):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(
        entity=entity, jsonify=True, json_encoder=BigQueryMetadataEncoder
    )
    assert metadata.metadata == ENCODED_METADATA


def test_nested_flatten_complex_types(partial_extract_metadata):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=FLATTEN)
    metadata = partial_extract_metadata(
        entity=entity, jsonify=True, flatten=True, json_encoder=BigQueryMetadataEncoder
    )
    assert metadata.metadata == FLATTEN_ENCODED_METADATA
