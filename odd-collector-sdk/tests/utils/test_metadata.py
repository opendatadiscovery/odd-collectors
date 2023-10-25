import json

import pytest
from attr import dataclass
from funcy import partial, walk_values
from odd_models import MetadataExtension
from pydantic import BaseModel

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata


@pytest.fixture
def partial_extract_metadata():
    return partial(
        extract_metadata,
        datasource="test_datasource",
        definition=DefinitionType.DATASET,
    )


COMPLEX_METADATA = {
    "meta": "meta",
    "nested_meta": {
        "nested_field_one": 1,
        "nested_field_two": "string",
        "nested_field_three": [{"foo": "bat"}, {"foo": {}}],
        "nested_field_four": {"more_nested_field": 1, "more_nested_field_two": {}, "more_nested_field_three": "string"},
        "nested_field_five": {},
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
    "nested_meta.nested_field_five": {},
}

ENCODED_METADATA = walk_values(lambda x: json.loads(json.dumps(x)), COMPLEX_METADATA)
FLATTEN_ENCODED_METADATA = walk_values(lambda x: json.loads(json.dumps(x)), FLATTEN)


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


def test_extract_metadata_from_class_without_metadata_field(partial_extract_metadata):
    entity = Entity(foo="foo", bar="bar")

    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == {}

    assert metadata.json()


def test_extract_metadata_from_dataclass_without_metadata_field(
    partial_extract_metadata,
):
    entity = DataclassEntity("foo", "bar")
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == {}

    assert metadata.json()


def test_extract_metadata_from_pydantic_without_metadata_field(
    partial_extract_metadata,
):
    entity = PydanticEntity(foo="foo", bar="bar")
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == {}

    assert metadata.json()


def test_extract_metadata_from_class_with_metadata_field(partial_extract_metadata):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata={"meta": "meta"})
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == entity.odd_metadata

    assert metadata.json()


def test_extract_metadata_from_class_with_metadata_added_at_runtime(
    partial_extract_metadata,
):
    entity = Entity(foo="foo", bar="bar")
    entity.odd_metadata = {"meta": "meta"}
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == entity.odd_metadata

    assert metadata.json()


def test_extract_metadata_from_dataclass_with_metadata_field(partial_extract_metadata):
    entity = DataclassEntityWithMetadata(
        foo="foo", bar="bar", odd_metadata={"meta": "meta"}
    )
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == entity.odd_metadata

    assert metadata.json()


def test_extract_metadata_from_dataclass_with_metadata_field_added_at_runtime(
    partial_extract_metadata,
):
    entity = DataclassEntity(foo="foo", bar="bar")
    entity.odd_metadata = {"meta": "meta"}
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == entity.odd_metadata

    assert metadata.json()


def test_extract_metadata_from_pydantic_class_with_metadata_field(
    partial_extract_metadata,
):
    entity = PydanticEntityWithMetadata(
        foo="foo", bar="bar", odd_metadata={"meta": "meta"}
    )
    metadata = partial_extract_metadata(entity=entity)

    assert isinstance(metadata, MetadataExtension)
    assert metadata.metadata == entity.odd_metadata

    assert metadata.json()


def test_extract_metadata_from_class_with_an_empty_with_complex(
    partial_extract_metadata,
):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(entity=entity)

    assert metadata.metadata == COMPLEX_METADATA
    assert metadata.json()


def test_extract_flatten_metadata_from_class_with_an_empty_with_complex(
    partial_extract_metadata,
):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(entity=entity, flatten=True)

    assert metadata.metadata == FLATTEN
    assert metadata.json()


def test_extract_jsonfy_metadata_from_class_with_an_empty_with_complex(
    partial_extract_metadata,
):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(entity=entity, jsonify=True)

    assert metadata.metadata == ENCODED_METADATA
    assert metadata.json()


def test_extract_flatten_jsonfy_metadata_from_class_with_an_empty_with_complex(
    partial_extract_metadata,
):
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(entity=entity, flatten=True, jsonify=True)

    assert metadata.metadata == FLATTEN_ENCODED_METADATA
    assert metadata.json()


def test_filter_none_values(partial_extract_metadata):
    metadata = COMPLEX_METADATA.copy()
    metadata["none_value"] = None
    entity = EntityWithMetadata(foo="foo", bar="bar", odd_metadata=COMPLEX_METADATA)
    metadata = partial_extract_metadata(entity=entity)
    assert "none_value" not in metadata.metadata


def test_flat_dict():
    from odd_collector_sdk.utils.metadata import flat_dict

    data = flat_dict(COMPLEX_METADATA)
    assert data == FLATTEN
