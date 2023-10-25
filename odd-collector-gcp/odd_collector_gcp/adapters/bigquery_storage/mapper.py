import json
from datetime import datetime
from functools import reduce
from operator import iconcat

from google.cloud.bigquery import (
    AvroOptions,
    BigtableColumn,
    BigtableColumnFamily,
    BigtableOptions,
    CSVOptions,
    ExternalConfig,
    ExternalSourceFormat,
    GoogleSheetsOptions,
    HivePartitioningOptions,
    ParquetOptions,
    RangePartitioning,
    TimePartitioning,
)
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityType,
    DataSet,
    DataSetField,
    DataSetFieldType,
    Type,
)
from oddrn_generator import BigQueryStorageGenerator

from odd_collector_gcp.adapters.bigquery_storage.dto import (
    BigQueryDataset,
    BigQueryField,
    BigQueryTable,
)
from odd_collector_gcp.utils.get_properties import get_properties

BIG_QUERY_STORAGE_TYPE_MAPPING = {
    "STRING": Type.TYPE_STRING,
    "BYTES": Type.TYPE_BINARY,
    "INT64": Type.TYPE_INTEGER,
    "INTEGER": Type.TYPE_INTEGER,
    "FLOAT64": Type.TYPE_NUMBER,
    "FLOAT": Type.TYPE_NUMBER,
    "NUMERIC": Type.TYPE_NUMBER,
    "BIGNUMERIC": Type.TYPE_NUMBER,
    "BOOLEAN": Type.TYPE_BOOLEAN,
    "BOOL": Type.TYPE_BOOLEAN,
    "TIMESTAMP": Type.TYPE_DATETIME,
    "DATE": Type.TYPE_DATETIME,
    "TIME": Type.TYPE_TIME,
    "DATETIME": Type.TYPE_DATETIME,
    "GEOGRAPHY": Type.TYPE_UNKNOWN,
    "ARRAY": Type.TYPE_LIST,
    "STRUCT": Type.TYPE_STRUCT,
    "RECORD": Type.TYPE_STRUCT,
}


class BigQueryStorageMapper:
    def __init__(self, oddrn_generator: BigQueryStorageGenerator):
        self.oddrn_generator = oddrn_generator

    def map_datasets(self, datasets: list[BigQueryDataset]) -> list[DataEntity]:
        return reduce(iconcat, [self.map_dataset(d) for d in datasets], [])

    def map_dataset(self, dataset_dto: BigQueryDataset) -> list[DataEntity]:
        dataset = dataset_dto.data_object
        self.oddrn_generator.set_oddrn_paths(datasets=dataset.dataset_id)

        tables = [self.map_table(BigQueryTable(t)) for t in dataset_dto.tables]

        database_service_deg = DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path("datasets"),
            name=dataset.dataset_id,
            description=dataset.description,
            metadata=[
                extract_metadata("bigquery", dataset_dto, DefinitionType.DATASET)
            ],
            created_at=dataset.created,
            updated_at=dataset.modified,
            type=DataEntityType.DATABASE_SERVICE,
            data_entity_group=DataEntityGroup(entities_list=[t.oddrn for t in tables]),
        )

        return tables + [database_service_deg]

    def map_table(self, table_dto: BigQueryTable) -> DataEntity:
        table = table_dto.data_object
        self.oddrn_generator.set_oddrn_paths(tables=table.table_id)
        fields = [BigQueryField(field) for field in table.schema]
        field_list = []
        for field in fields:
            field_mapper = FieldMapper(self.oddrn_generator, field)
            processed_ds_fields = field_mapper.dataset_fields
            field_list.extend(processed_ds_fields)
        return DataEntity(
            oddrn=self.oddrn_generator.get_oddrn_by_path("tables"),
            name=table.table_id,
            description=table.description,
            metadata=[
                extract_metadata(
                    "bigquery",
                    table_dto,
                    DefinitionType.DATASET,
                    flatten=True,
                    jsonify=True,
                    json_encoder=BigQueryMetadataEncoder,
                )
            ],
            created_at=table.created,
            updated_at=table.modified,
            type=DataEntityType.TABLE,
            dataset=DataSet(rows_number=table.num_rows, field_list=field_list),
        )


class FieldMapper:
    def __init__(
        self, oddrn_generator: BigQueryStorageGenerator, field_dto: BigQueryField
    ):
        self.oddrn_generator = oddrn_generator
        self.dataset_fields = []
        self.map_field(field_dto)

    def map_field(
        self,
        field_dto: BigQueryField,
        parent_oddrn: str = None,
        is_array_element: bool = False,
    ):
        field = field_dto.data_object
        if parent_oddrn:
            oddrn = f"{parent_oddrn}/keys/{field.name}"
        else:
            oddrn = self.oddrn_generator.get_oddrn_by_path("columns", field.name)

        if field.mode == "REPEATED" and not is_array_element:
            field_type = "ARRAY"
            array_field = DataSetField(
                oddrn=oddrn,
                name=field.name,
                description=field.description,
                parent_field_oddrn=parent_oddrn,
                metadata=[
                    extract_metadata(
                        "bigquery", field_dto, DefinitionType.DATASET_FIELD
                    )
                ],
                type=DataSetFieldType(
                    type=BIG_QUERY_STORAGE_TYPE_MAPPING.get(
                        field_type, Type.TYPE_UNKNOWN
                    ),
                    logical_type=field_type,
                    is_nullable=field.is_nullable,
                ),
            )
            self.dataset_fields.append(array_field)
            self.map_field(BigQueryField(field), oddrn, True)
        else:
            field_name = "Element" if is_array_element else field.name
            field_entity = DataSetField(
                oddrn=oddrn,
                name=field_name,
                description=field.description,
                parent_field_oddrn=parent_oddrn,
                metadata=[
                    extract_metadata(
                        "bigquery", field_dto, DefinitionType.DATASET_FIELD
                    )
                ],
                type=DataSetFieldType(
                    type=BIG_QUERY_STORAGE_TYPE_MAPPING.get(
                        field.field_type, Type.TYPE_UNKNOWN
                    ),
                    logical_type=field.field_type,
                    is_nullable=field.is_nullable,
                ),
            )
            self.dataset_fields.append(field_entity)
            if field.field_type == "RECORD":
                for f in field.fields:
                    self.map_field(BigQueryField(f), field_entity.oddrn)


class BigQueryMetadataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(
            obj,
            (
                ExternalConfig,
                ParquetOptions,
                TimePartitioning,
                AvroOptions,
                RangePartitioning,
                BigtableOptions,
                BigtableColumnFamily,
                BigtableColumn,
                CSVOptions,
                GoogleSheetsOptions,
                ExternalSourceFormat,
                HivePartitioningOptions,
            ),
        ):
            return get_properties(obj)
        return super().default(obj)
