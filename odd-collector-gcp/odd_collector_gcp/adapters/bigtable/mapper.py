import datetime
from typing import List

from funcy import lmapcat
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataEntityGroup,
    DataSet,
    DataSetField,
    DataSetFieldType,
    Type,
)
from oddrn_generator import BigTableGenerator

from odd_collector_gcp.adapters.bigtable.models import (
    BigTableInstance,
    BigTableTable,
    BigTableColumn,
)


TYPE_MAPPING = {
    str: Type.TYPE_STRING,
    float: Type.TYPE_NUMBER,
    int: Type.TYPE_INTEGER,
    bool: Type.TYPE_BOOLEAN,
    datetime.datetime: Type.TYPE_DATETIME,
    datetime.time: Type.TYPE_TIME,
    dict: Type.TYPE_STRUCT,
    bytes: Type.TYPE_BINARY,
    list: Type.TYPE_LIST,
    tuple: Type.TYPE_UNION,
    datetime.timedelta: Type.TYPE_DURATION,
}


class BigTableMapper:
    def __init__(self, oddrn_generator: BigTableGenerator):
        self.__oddrn_generator = oddrn_generator

    def map_instances(self, instances: List[BigTableInstance]) -> List[DataEntity]:
        return lmapcat(self.map_instance, instances)

    def map_instance(self, instance: BigTableInstance) -> List[DataEntity]:
        self.__oddrn_generator.set_oddrn_paths(instances=instance.instance_id)
        tables = [self.map_table(t) for t in instance.tables]
        database_service_deg = DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("instances"),
            name=instance.instance_id,
            metadata=[],
            type=DataEntityType.DATABASE_SERVICE,
            data_entity_group=DataEntityGroup(entities_list=[t.oddrn for t in tables]),
        )
        return tables + [database_service_deg]

    def map_table(self, table: BigTableTable) -> DataEntity:
        self.__oddrn_generator.set_oddrn_paths(tables=table.table_id)

        return DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("tables"),
            name=table.table_id,
            metadata=[],
            type=DataEntityType.TABLE,
            dataset=DataSet(field_list=self.map_columns(table.columns)),
        )

    def map_columns(
        self,
        columns: List[BigTableColumn],
    ) -> List[DataSetField]:
        fields = []
        for column in columns:
            field = DataSetField(
                oddrn=self.__oddrn_generator.get_oddrn_by_path("columns", column.name),
                name=column.name,
                metadata=[],
                type=DataSetFieldType(
                    type=TYPE_MAPPING.get(
                        type(column.value.decode()), Type.TYPE_UNKNOWN
                    ),
                    logical_type="bytearray",
                    is_nullable=True,
                ),
            )
            fields.append(field)
        return fields
