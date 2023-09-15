from typing import List

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from google.cloud import bigtable
from oddrn_generator import BigTableGenerator

from odd_collector_gcp.adapters.bigtable.mapper import BigTableMapper
from odd_collector_gcp.adapters.bigtable.models import (
    BigTableInstance,
    BigTableTable,
    BigTableColumn,
)
from odd_collector_gcp.domain.plugin import BigTablePlugin


class Adapter(BaseAdapter):
    config: BigTablePlugin
    generator: BigTableGenerator

    def __init__(self, config: BigTablePlugin):
        super().__init__(config)

    def create_generator(self) -> BigTableGenerator:
        return BigTableGenerator(google_cloud_settings={"project": self.config.project})

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        mapper = BigTableMapper(oddrn_generator=self.generator)
        entities = mapper.map_instances(self.__get_instances())

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )

    def __get_instances(self) -> List[BigTableInstance]:
        client = bigtable.Client(project=self.config.project, admin=True)
        instances = []
        # list_instances() -> (instances, failed_locations).
        for instance in client.list_instances()[0]:
            tables = []
            for table in instance.list_tables():
                merged_columns = {}
                columns = []
                # get combination of all types in table used across the first N rows.
                for row in table.read_rows(limit=self.config.rows_limit):
                    merged_columns = merged_columns | row.to_dict()
                for col_name, col_val in merged_columns.items():
                    col_val = col_val[0].value if any(col_val) else None
                    columns.append(
                        BigTableColumn(name=col_name.decode(), value=col_val)
                    )
                tables.append(BigTableTable(table_id=table.table_id, columns=columns))
            instances.append(
                BigTableInstance(
                    instance_id=instance.instance_id,
                    tables=tables,
                )
            )
        return instances
