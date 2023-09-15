from dataclasses import dataclass
from typing import Any

from google.cloud.bigquery import Dataset, SchemaField, Table
from odd_collector_sdk.utils.metadata import HasMetadata

from odd_collector_gcp.utils.get_properties import get_properties


class MetadataMixin:
    data_object: Any
    excluded_properties = []

    @property
    def odd_metadata(self) -> dict:
        return get_properties(self.data_object, self.excluded_properties)


@dataclass
class BigQueryDataset(MetadataMixin, HasMetadata):
    data_object: Dataset
    tables: list[Table]
    excluded_properties = [
        "reference",
        "description",
        "dataset_id",
        "created",
        "updated",
        "access_entries",
    ]


@dataclass
class BigQueryTable(MetadataMixin, HasMetadata):
    data_object: Table
    excluded_properties = [
        "reference",
        "schema",
        "num_rows",
        "description",
        "table_id",
        "created",
        "updated",
    ]


@dataclass
class BigQueryField(MetadataMixin, HasMetadata):
    data_object: SchemaField
    excluded_properties = [
        "name",
        "field_type",
        "fields",
        "description",
        "is_nullable",
    ]
