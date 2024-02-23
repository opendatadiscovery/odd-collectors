from odd_collector.adapters.postgresql.mappers.relationships.relationship_mapper import (
    RelationshipMapper,
)
from odd_collector.adapters.postgresql.mappers.relationships.utils import (
    group_uniques_constraints_by_table,
)
from odd_collector.adapters.postgresql.models import (
    ForeignKeyConstraint,
    UniqueConstraint,
)
from odd_models.models.models import DataEntity
from oddrn_generator.generators import PostgresqlGenerator


class DataEntityRelationshipsMapper:
    def __init__(
        self,
        oddrn_generator: PostgresqlGenerator,
        unique_constraints: list[UniqueConstraint],
        datasets: dict[str, DataEntity],
    ):
        self.oddrn_generator = oddrn_generator
        self.unique_constraints = group_uniques_constraints_by_table(unique_constraints)
        self.datasets = datasets

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
            unique_constraints=self._get_unique_constraints(schema_name, table_name),
        ).build_data_entity()

    def _get_dataset(self, schema_name: str, table_name: str) -> DataEntity:
        return self.datasets[f"{schema_name}.{table_name}"]

    def _get_unique_constraints(
        self, schema_name: str, table_name: str
    ) -> list[UniqueConstraint]:
        return self.unique_constraints.get(f"{schema_name}.{table_name}", [])
