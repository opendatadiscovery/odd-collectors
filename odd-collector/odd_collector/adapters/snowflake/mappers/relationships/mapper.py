from odd_models.models.models import DataEntity
from oddrn_generator.generators import SnowflakeGenerator

from odd_collector.adapters.snowflake.mappers.relationships.relationship_mapper import (
    RelationshipMapper,
)
from odd_collector.adapters.snowflake.domain import (
    Table,
    ForeignKeyConstraint,
)


class DataEntityRelationshipsMapper:
    def __init__(
        self,
        oddrn_generator: SnowflakeGenerator,
        table_entities_pair: list[tuple[Table, DataEntity]],
    ):
        self.oddrn_generator = oddrn_generator
        self.datasets = self._map_datasets_by_full_name(table_entities_pair)

    def map(self, data: list[ForeignKeyConstraint]) -> list[DataEntity]:
        return [self._map_data_entity_relationship(fkc) for fkc in data]

    def _map_data_entity_relationship(
        self, fk_cons: ForeignKeyConstraint
    ) -> DataEntity:
        schema_name = fk_cons.schema_name
        table_name = fk_cons.table_name
        referenced_schema_name = fk_cons.referenced_schema_name
        referenced_table_name = fk_cons.referenced_table_name

        self.oddrn_generator.set_oddrn_paths(
            schemas=schema_name,
            tables=table_name,
            relationships=f"references_{referenced_table_name}_with_{fk_cons.constraint_name}",
        )

        return RelationshipMapper(
            fk_cons=fk_cons,
            oddrn=self.oddrn_generator.get_oddrn_by_path("relationships"),
            source=self._get_dataset(schema_name, table_name),
            target=self._get_dataset(referenced_schema_name, referenced_table_name),
        ).build_data_entity()

    @staticmethod
    def _map_datasets_by_full_name(
        table_entities_pair: list[tuple[Table, DataEntity]]
    ) -> dict[str, DataEntity]:
        return {
            f"{te_pair[0].table_schema}.{te_pair[0].table_name}": te_pair[1]
            for te_pair in table_entities_pair
        }

    def _get_dataset(self, schema_name: str, table_name: str) -> DataEntity:
        return self.datasets[f"{schema_name}.{table_name}"]
